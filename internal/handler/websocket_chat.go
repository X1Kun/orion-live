package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/X1Kun/orion-live/internal/service"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/X1Kun/orion-live/pkg/logger"
)

var errWebSocketOutboundQueueFull = errors.New("WebSocket outbound queue is full")

type chatSendFrame struct {
	Type      string `json:"type"`
	MessageID string `json:"message_id"`
	Content   string `json:"content"`
}

type chatAckFrame struct {
	Type       string          `json:"type"`
	MessageID  string          `json:"message_id,omitempty"`
	Status     string          `json:"status"`
	AcceptedAt *time.Time      `json:"accepted_at,omitempty"`
	Error      *webSocketError `json:"error,omitempty"`
}

type webSocketError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *WebSocketHandler) handleClientFrame(ctx context.Context, client *roomhub.Client, liveSessionID, userID uint64, body []byte) error {
	frame, err := decodeClientFrame(body)
	if err != nil {
		return enqueueChatAck(client, chatAckFrame{
			Type: "chat.ack", Status: "rejected",
			Error: &webSocketError{Code: "INVALID_FRAME", Message: "frame must be a valid chat.send message"},
		})
	}
	if frame.Type != "chat.send" {
		return enqueueChatAck(client, chatAckFrame{
			Type: "chat.ack", MessageID: frame.MessageID, Status: "rejected",
			Error: &webSocketError{Code: "UNSUPPORTED_MESSAGE_TYPE", Message: "message type is not supported"},
		})
	}
	if !h.hub.SendAllowed(liveSessionID) {
		return enqueueChatAck(client, chatAckFrame{
			Type: "chat.ack", MessageID: frame.MessageID, Status: "rejected",
			Error: &webSocketError{Code: "LIVE_SESSION_ENDED", Message: "live session no longer accepts messages"},
		})
	}

	accepted, err := h.chat.Accept(ctx, liveSessionID, userID, frame.MessageID, frame.Content)
	if err != nil {
		code, message := "CHAT_UNAVAILABLE", "message could not be accepted"
		switch {
		case errors.Is(err, service.ErrInvalidChatMessageID), errors.Is(err, service.ErrInvalidChatContent):
			code, message = "INVALID_MESSAGE", err.Error()
		case errors.Is(err, service.ErrChatRateLimited):
			code, message = "CHAT_RATE_LIMITED", "Chat rate limit exceeded"
		default:
			logger.Log.WithError(err).
				WithField("live_session_id", liveSessionID).
				WithField("user_id", userID).
				Warn("publish Chat message")
		}
		return enqueueChatAck(client, chatAckFrame{
			Type: "chat.ack", MessageID: frame.MessageID, Status: "rejected",
			Error: &webSocketError{Code: code, Message: message},
		})
	}
	return enqueueChatAck(client, chatAckFrame{
		Type: "chat.ack", MessageID: accepted.MessageID, Status: "accepted", AcceptedAt: &accepted.AcceptedAt,
	})
}

func decodeClientFrame(body []byte) (chatSendFrame, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var frame chatSendFrame
	if err := decoder.Decode(&frame); err != nil {
		return chatSendFrame{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return chatSendFrame{}, errors.New("frame must contain one JSON object")
	}
	return frame, nil
}

func enqueueChatAck(client *roomhub.Client, response chatAckFrame) error {
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if !client.EnqueueOutbound(body) {
		return errWebSocketOutboundQueueFull
	}
	return nil
}
