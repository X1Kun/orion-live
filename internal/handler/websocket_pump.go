package handler

import (
	"context"
	"errors"
	"time"

	"github.com/X1Kun/orion-live/internal/metrics"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	gorilla "github.com/gorilla/websocket"
)

var (
	errWebSocketMessageTypeUnsupported = errors.New("WebSocket message type is not supported")
	errWebSocketInboundQueueFull       = errors.New("WebSocket inbound queue is full")
)

func (h *WebSocketHandler) readPump(connection *gorilla.Conn, inbound chan<- []byte) error {
	defer close(inbound)
	connection.SetReadLimit(h.config.ReadLimitBytes)
	if err := connection.SetReadDeadline(time.Now().Add(h.config.PongTimeout)); err != nil {
		return err
	}
	connection.SetPongHandler(func(string) error {
		return connection.SetReadDeadline(time.Now().Add(h.config.PongTimeout))
	})

	for {
		messageType, body, err := connection.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != gorilla.TextMessage {
			return errWebSocketMessageTypeUnsupported
		}
		select {
		case inbound <- body:
		default:
			return errWebSocketInboundQueueFull
		}
	}
}

func (h *WebSocketHandler) processPump(ctx context.Context, inbound <-chan []byte, client *roomhub.Client, liveSessionID, userID uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case body, ok := <-inbound:
			if !ok {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			started := time.Now()
			err := h.handleClientFrame(ctx, client, liveSessionID, userID, body)
			metrics.WebSocketFrameProcessingDuration.Observe(time.Since(started).Seconds())
			if err != nil {
				return err
			}
		}
	}
}

func (h *WebSocketHandler) writePump(connection *gorilla.Conn, client *roomhub.Client) error {
	pingTicker := time.NewTicker(h.config.PingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case message := <-client.Outbound():
			if err := connection.SetWriteDeadline(time.Now().Add(h.config.WriteTimeout)); err != nil {
				return err
			}
			if err := connection.WriteMessage(gorilla.TextMessage, message); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := connection.WriteControl(gorilla.PingMessage, nil, time.Now().Add(h.config.WriteTimeout)); err != nil {
				return err
			}
		case <-client.Done():
			return connection.WriteControl(
				gorilla.CloseMessage,
				gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, ""),
				time.Now().Add(h.config.WriteTimeout),
			)
		}
	}
}

func writeWebSocketClose(connection *gorilla.Conn, code int, message string, timeout time.Duration) {
	_ = connection.WriteControl(
		gorilla.CloseMessage,
		gorilla.FormatCloseMessage(code, message),
		time.Now().Add(timeout),
	)
}

func unexpectedWebSocketError(err error) bool {
	return err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, errWebSocketMessageTypeUnsupported) &&
		!errors.Is(err, errWebSocketOutboundQueueFull) &&
		!gorilla.IsCloseError(err, gorilla.CloseNormalClosure, gorilla.CloseGoingAway, gorilla.CloseNoStatusReceived)
}
