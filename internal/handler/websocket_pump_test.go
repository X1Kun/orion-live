package handler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/service"
	gorilla "github.com/gorilla/websocket"
)

type controlledChatService struct {
	entered    chan string
	release    chan struct{}
	canceled   chan struct{}
	cancelOnce sync.Once
}

func TestWebSocketShutdownCancelsSlowProcessor(t *testing.T) {
	server, _, handler := newWebSocketTestServer(t, nil)
	chat := &controlledChatService{entered: make(chan string, 1), release: make(chan struct{}), canceled: make(chan struct{})}
	handler.chat = chat
	conn := dialWebSocketTest(t, server.URL, 7, 42)
	defer conn.Close()
	if err := conn.WriteJSON(chatSendFrame{Type: "chat.send", MessageID: "first", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-chat.entered:
	case <-time.After(time.Second):
		t.Fatal("processor did not begin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() did not finish: %v", err)
	}
	select {
	case <-chat.canceled:
	default:
		t.Fatal("Shutdown returned before processing was canceled")
	}
}

func (s *controlledChatService) Accept(ctx context.Context, _, _ uint64, id, _ string) (service.ChatAcceptance, error) {
	s.entered <- id
	select {
	case <-ctx.Done():
		s.cancelOnce.Do(func() { close(s.canceled) })
		return service.ChatAcceptance{}, ctx.Err()
	case <-s.release:
		return service.ChatAcceptance{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
	}
}

func TestWebSocketSlowChatKeepsHeartbeatAndOrder(t *testing.T) {
	cfg := webSocketTestConfig()
	cfg.PingInterval = 30 * time.Millisecond
	cfg.PongTimeout = 300 * time.Millisecond
	server, _, handler := newWebSocketTestServerWithConfig(t, nil, cfg)
	chat := &controlledChatService{entered: make(chan string, 4), release: make(chan struct{}), canceled: make(chan struct{})}
	handler.chat = chat
	conn := dialWebSocketTest(t, server.URL, 7, 42)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	acks := make(chan string, 4)
	readErr := make(chan error, 1)
	go func() {
		for {
			_, body, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			var ack chatAckFrame
			if err = json.Unmarshal(body, &ack); err == nil && ack.Type == "chat.ack" {
				acks <- ack.MessageID
			}
		}
	}()
	for _, id := range []string{"first", "second"} {
		if err := conn.WriteJSON(chatSendFrame{Type: "chat.send", MessageID: id, Content: "hello"}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case id := <-chat.entered:
		if id != "first" {
			t.Fatalf("first processed = %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("processing did not begin")
	}
	select {
	case err := <-readErr:
		t.Fatalf("connection closed during slow business processing: %v", err)
	case <-time.After(650 * time.Millisecond):
	}
	select {
	case id := <-chat.entered:
		t.Fatalf("second message processed before first completed: %s", id)
	default:
	}
	close(chat.release)
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-acks:
			if got != want {
				t.Fatalf("ACK = %s, want %s", got, want)
			}
		case err := <-readErr:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatal("missing ACK")
		}
	}
	_ = conn.Close()
	select {
	case <-readErr:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop")
	}
}

func TestWebSocketInboundOverflowClosesAndCancelsProcessor(t *testing.T) {
	cfg := webSocketTestConfig()
	cfg.ClientReceiveQueueCapacity = 1
	server, _, handler := newWebSocketTestServerWithConfig(t, nil, cfg)
	chat := &controlledChatService{entered: make(chan string, 4), release: make(chan struct{}), canceled: make(chan struct{})}
	handler.chat = chat
	conn := dialWebSocketTest(t, server.URL, 7, 42)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn.WriteJSON(chatSendFrame{Type: "chat.send", MessageID: "first", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-chat.entered:
	case <-time.After(time.Second):
		t.Fatal("processor did not start")
	}
	for _, id := range []string{"second", "third"} {
		if err := conn.WriteJSON(chatSendFrame{Type: "chat.send", MessageID: id, Content: "hello"}); err != nil {
			t.Fatal(err)
		}
	}
	for {
		_, _, err := conn.ReadMessage()
		if err == nil {
			continue
		}
		var closeErr *gorilla.CloseError
		if !errors.As(err, &closeErr) || closeErr.Code != gorilla.CloseTryAgainLater {
			t.Fatalf("close error = %v, want 1013", err)
		}
		break
	}
	select {
	case <-chat.canceled:
	case <-time.After(time.Second):
		t.Fatal("in-flight business context not canceled")
	}
	select {
	case id := <-chat.entered:
		t.Fatalf("queued message processed after cancellation: %s", id)
	default:
	}
}
