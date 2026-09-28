package handler

import (
	"context"
	"errors"
	"time"

	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	gorilla "github.com/gorilla/websocket"
)

var errWebSocketMessageTypeUnsupported = errors.New("WebSocket message type is not supported")

func (h *WebSocketHandler) readPump(ctx context.Context, connection *gorilla.Conn, client *roomhub.Client, liveSessionID, userID uint64) error {
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
		if err := h.handleClientFrame(ctx, client, liveSessionID, userID, body); err != nil {
			return err
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
		!errors.Is(err, errWebSocketMessageTypeUnsupported) &&
		!errors.Is(err, errWebSocketOutboundQueueFull) &&
		!gorilla.IsCloseError(err, gorilla.CloseNormalClosure, gorilla.CloseGoingAway, gorilla.CloseNoStatusReceived)
}
