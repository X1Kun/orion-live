package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	gorilla "github.com/gorilla/websocket"
)

type loadClient struct {
	index          int
	connection     *gorilla.Conn
	requestTimeout time.Duration
	collector      *collector
}

type acknowledgement struct {
	Type      string `json:"type"`
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
	Error     *struct {
		Code string `json:"code"`
	} `json:"error,omitempty"`
}

func connectClient(ctx context.Context, api *apiClient, sessionID uint64, token string, index int, timeout time.Duration, results *collector) (*loadClient, error) {
	dialer := *gorilla.DefaultDialer
	dialer.HandshakeTimeout = timeout
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	connection, response, err := dialer.DialContext(ctx, api.webSocketURL(sessionID), header)
	if err != nil {
		status := "no HTTP response"
		if response != nil {
			status = response.Status
			_ = response.Body.Close()
		}
		return nil, fmt.Errorf("connect client %d: %w (%s)", index, err, status)
	}
	fail := func(err error) (*loadClient, error) {
		_ = connection.Close()
		return nil, err
	}
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return fail(err)
	}
	_, body, err := connection.ReadMessage()
	if err != nil {
		return fail(fmt.Errorf("read room.ready for client %d: %w", index, err))
	}
	var ready struct {
		Type          string `json:"type"`
		LiveSessionID uint64 `json:"live_session_id"`
	}
	if err := json.Unmarshal(body, &ready); err != nil || ready.Type != "room.ready" || ready.LiveSessionID != sessionID {
		return fail(fmt.Errorf("client %d received invalid room.ready: %s", index, body))
	}
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return fail(err)
	}
	return &loadClient{index: index, connection: connection, requestTimeout: timeout, collector: results}, nil
}

func (c *loadClient) send(messageID, content string) error {
	if err := c.connection.SetWriteDeadline(time.Now().Add(c.requestTimeout)); err != nil {
		return err
	}
	return c.connection.WriteJSON(map[string]string{
		"type": "chat.send", "message_id": messageID, "content": content,
	})
}

func (c *loadClient) read(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	for {
		_, body, err := c.connection.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				c.collector.setError(fmt.Errorf("read client %d: %w", c.index, err))
			}
			return
		}
		if err := c.handleFrame(body, time.Now()); err != nil {
			c.collector.setError(fmt.Errorf("client %d: %w", c.index, err))
			return
		}
	}
}

func (c *loadClient) handleFrame(body []byte, observedAt time.Time) error {
	var frameType struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &frameType); err != nil {
		return fmt.Errorf("decode server frame: %w", err)
	}
	if frameType.Type != "" {
		if frameType.Type != "chat.ack" {
			return fmt.Errorf("unexpected server frame type %q", frameType.Type)
		}
		var ack acknowledgement
		if err := json.Unmarshal(body, &ack); err != nil {
			return fmt.Errorf("decode Chat ACK: %w", err)
		}
		code := ""
		if ack.Error != nil {
			code = ack.Error.Code
		}
		c.collector.recordAck(ack.MessageID, ack.Status, code, observedAt)
		return nil
	}

	var event messaging.Event
	if err := json.Unmarshal(body, &event); err != nil {
		return fmt.Errorf("decode realtime event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate realtime event: %w", err)
	}
	if event.EventType != messaging.EventTypeChatMessageAccepted {
		return fmt.Errorf("unexpected realtime event type %q", event.EventType)
	}
	var payload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode Chat event payload: %w", err)
	}
	if err := payload.Validate(); err != nil {
		return fmt.Errorf("validate Chat event payload: %w", err)
	}
	c.collector.recordEvent(c.index, payload.MessageID, observedAt)
	return nil
}

func closeClients(clients []*loadClient) {
	for _, client := range clients {
		if client != nil {
			_ = client.connection.Close()
		}
	}
}
