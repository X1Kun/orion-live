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

type inboundFrame struct {
	body       []byte
	enqueuedAt time.Time
}

func (h *WebSocketHandler) readPump(connection *gorilla.Conn, inbound chan<- inboundFrame) error {
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
		case inbound <- inboundFrame{body: body, enqueuedAt: time.Now()}:
		default:
			return errWebSocketInboundQueueFull
		}
	}
}

func (h *WebSocketHandler) processPump(ctx context.Context, inbound <-chan inboundFrame, client *roomhub.Client, liveSessionID, userID uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-inbound:
			if !ok {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			started := time.Now()
			metrics.WebSocketQueueWait.WithLabelValues("inbound").Observe(started.Sub(frame.enqueuedAt).Seconds())
			err := h.handleClientFrame(ctx, client, liveSessionID, userID, frame.body)
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
			metrics.WebSocketQueueWait.WithLabelValues("outbound").Observe(time.Since(message.EnqueuedAt).Seconds())
			if err := connection.SetWriteDeadline(time.Now().Add(h.config.WriteTimeout)); err != nil {
				return err
			}
			if err := connection.WriteMessage(gorilla.TextMessage, message.Body); err != nil {
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
