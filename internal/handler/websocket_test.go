package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/middleware"
	"github.com/X1Kun/orion-live/internal/service"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	gorilla "github.com/gorilla/websocket"
)

const webSocketTestSecret = "test-secret-with-at-least-32-characters"

func TestWebSocketConnectRequiresAuthenticationAndLiveSession(t *testing.T) {
	tests := []struct {
		name       string
		token      bool
		origin     string
		admission  error
		wantStatus int
	}{
		{name: "missing authentication", wantStatus: http.StatusUnauthorized},
		{name: "scheduled session", token: true, admission: service.ErrLiveSessionNotLive, wantStatus: http.StatusConflict},
		{name: "missing session", token: true, admission: service.ErrLiveSessionNotFound, wantStatus: http.StatusNotFound},
		{name: "cross-origin request", token: true, origin: "https://example.invalid", wantStatus: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, _ := newWebSocketTestServer(t, tt.admission)
			headers := http.Header{}
			if tt.token {
				headers.Set("Authorization", "Bearer "+webSocketTestToken(t, 42))
			}
			if tt.origin != "" {
				headers.Set("Origin", tt.origin)
			}
			connection, response, err := gorilla.DefaultDialer.Dial(webSocketURL(server.URL, 1), headers)
			if connection != nil {
				_ = connection.Close()
			}
			if err == nil {
				t.Fatal("Dial() unexpectedly succeeded")
			}
			if response == nil || response.StatusCode != tt.wantStatus {
				t.Fatalf("handshake status = %v, want %d", responseStatus(response), tt.wantStatus)
			}
		})
	}
}

func TestWebSocketConnectJoinsHubAndReceivesBroadcast(t *testing.T) {
	server, hub, _ := newWebSocketTestServer(t, nil)
	headers := http.Header{"Authorization": []string{"Bearer " + webSocketTestToken(t, 42)}}
	connection, response, err := gorilla.DefaultDialer.Dial(webSocketURL(server.URL, 7), headers)
	if err != nil {
		t.Fatalf("Dial() error = %v, response status = %v", err, responseStatus(response))
	}
	defer connection.Close()

	waitForWebSocketTest(t, func() bool { return hub.ClientCount(7) == 1 }, "client to join room")
	present, err := hub.BroadcastIfPresent(7, []byte(`{"type":"test.event"}`))
	if err != nil || !present {
		t.Fatalf("BroadcastIfPresent() present = %v, error = %v", present, err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	messageType, message, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != gorilla.TextMessage || string(message) != `{"type":"test.event"}` {
		t.Fatalf("message type = %d, body = %q", messageType, message)
	}

	if err := connection.WriteMessage(gorilla.TextMessage, []byte(`{"type":"chat.send"}`)); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	_, _, err = connection.ReadMessage()
	var closeError *gorilla.CloseError
	if !errors.As(err, &closeError) || closeError.Code != gorilla.CloseUnsupportedData {
		t.Fatalf("client message close error = %v, want code %d", err, gorilla.CloseUnsupportedData)
	}
	waitForWebSocketTest(t, func() bool { return hub.RoomCount() == 0 }, "client to leave room")
}

func TestWebSocketHubShutdownClosesConnection(t *testing.T) {
	server, hub, webSockets := newWebSocketTestServer(t, nil)
	headers := http.Header{"Authorization": []string{"Bearer " + webSocketTestToken(t, 42)}}
	connection, response, err := gorilla.DefaultDialer.Dial(webSocketURL(server.URL, 7), headers)
	if err != nil {
		t.Fatalf("Dial() error = %v, response status = %v", err, responseStatus(response))
	}
	defer connection.Close()
	waitForWebSocketTest(t, func() bool { return hub.ClientCount(7) == 1 }, "client to join room")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := webSockets.Shutdown(ctx); err != nil {
		t.Fatalf("WebSocketHandler.Shutdown() error = %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, _, err = connection.ReadMessage()
	var closeError *gorilla.CloseError
	if !errors.As(err, &closeError) || closeError.Code != gorilla.CloseNormalClosure {
		t.Fatalf("shutdown close error = %v, want code %d", err, gorilla.CloseNormalClosure)
	}
}

func TestWebSocketConnectionAdmissionLimits(t *testing.T) {
	cfg := webSocketTestConfig()
	cfg.MaxConnections = 2
	cfg.MaxConnectionsPerUser = 1
	server, hub, webSockets := newWebSocketTestServerWithConfig(t, nil, cfg)

	first := dialWebSocketTest(t, server.URL, 7, 42)
	defer first.Close()
	waitForWebSocketTest(t, func() bool { return hub.ClientCount(7) == 1 }, "first client to join")

	assertWebSocketHandshakeStatus(t, server.URL, 7, 42, http.StatusTooManyRequests)
	secondUser := dialWebSocketTest(t, server.URL, 7, 43)
	defer secondUser.Close()
	waitForWebSocketTest(t, func() bool { return hub.ClientCount(7) == 2 }, "second user to join")
	assertWebSocketHandshakeStatus(t, server.URL, 7, 44, http.StatusServiceUnavailable)

	writeClientClose(t, first)
	waitForWebSocketTest(t, func() bool {
		return hub.ClientCount(7) == 1 && activeWebSocketConnectionsForUser(webSockets, 42) == 0
	}, "first user admission to be released")
	reconnected := dialWebSocketTest(t, server.URL, 7, 42)
	defer reconnected.Close()
}

func TestWebSocketDrainingRejectsHandshake(t *testing.T) {
	server, _, webSockets := newWebSocketTestServer(t, nil)
	webSockets.SetDraining()
	assertWebSocketHandshakeStatus(t, server.URL, 7, 42, http.StatusServiceUnavailable)
}

func newWebSocketTestServer(t *testing.T, admissionError error) (*httptest.Server, *roomhub.Hub, *WebSocketHandler) {
	return newWebSocketTestServerWithConfig(t, admissionError, webSocketTestConfig())
}

func newWebSocketTestServerWithConfig(t *testing.T, admissionError error, cfg config.WebSocket) (*httptest.Server, *roomhub.Hub, *WebSocketHandler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	hub, err := roomhub.NewHub(8)
	if err != nil {
		t.Fatalf("NewHub() error = %v", err)
	}
	sessions := &liveSessionServiceStub{
		authorizeJoin: func(_ context.Context, _, _ uint64) error {
			return admissionError
		},
	}
	webSockets := NewWebSocketHandler(sessions, hub, cfg)
	router := gin.New()
	router.GET("/api/v1/live-sessions/:id/ws", middleware.Auth(webSocketTestSecret), webSockets.Connect)
	server := httptest.NewServer(router)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := webSockets.Shutdown(ctx); err != nil {
			t.Errorf("WebSocketHandler.Shutdown() error = %v", err)
		}
		server.Close()
	})
	return server, hub, webSockets
}

func webSocketTestConfig() config.WebSocket {
	return config.WebSocket{
		HandshakeTimeout:           time.Second,
		WriteTimeout:               time.Second,
		PongTimeout:                time.Minute,
		PingInterval:               30 * time.Second,
		ReadLimitBytes:             4096,
		ClientSendQueueCapacity:    8,
		RoomBroadcastQueueCapacity: 8,
		MaxConnections:             16,
		MaxConnectionsPerUser:      4,
	}
}

func dialWebSocketTest(t *testing.T, serverURL string, liveSessionID, userID uint64) *gorilla.Conn {
	t.Helper()
	headers := http.Header{"Authorization": []string{"Bearer " + webSocketTestToken(t, userID)}}
	connection, response, err := gorilla.DefaultDialer.Dial(webSocketURL(serverURL, liveSessionID), headers)
	if err != nil {
		t.Fatalf("Dial() error = %v, response status = %v", err, responseStatus(response))
	}
	return connection
}

func assertWebSocketHandshakeStatus(t *testing.T, serverURL string, liveSessionID, userID uint64, wantStatus int) {
	t.Helper()
	headers := http.Header{"Authorization": []string{"Bearer " + webSocketTestToken(t, userID)}}
	connection, response, err := gorilla.DefaultDialer.Dial(webSocketURL(serverURL, liveSessionID), headers)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("Dial() unexpectedly succeeded")
	}
	if response == nil || response.StatusCode != wantStatus {
		t.Fatalf("handshake status = %v, want %d", responseStatus(response), wantStatus)
	}
}

func writeClientClose(t *testing.T, connection *gorilla.Conn) {
	t.Helper()
	if err := connection.WriteControl(
		gorilla.CloseMessage,
		gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, ""),
		time.Now().Add(time.Second),
	); err != nil {
		t.Fatalf("write client close: %v", err)
	}
}

func activeWebSocketConnectionsForUser(handler *WebSocketHandler, userID uint64) int {
	handler.lifecycleMu.Lock()
	defer handler.lifecycleMu.Unlock()
	return handler.activeByUser[userID]
}

func webSocketTestToken(t *testing.T, userID uint64) string {
	t.Helper()
	now := time.Now().UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   strconv.FormatUint(userID, 10),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
	})
	signed, err := token.SignedString([]byte(webSocketTestSecret))
	if err != nil {
		t.Fatalf("sign token for user %d: %v", userID, err)
	}
	return signed
}

func webSocketURL(serverURL string, liveSessionID uint64) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + "/api/v1/live-sessions/" + strconv.FormatUint(liveSessionID, 10) + "/ws"
}

func responseStatus(response *http.Response) any {
	if response == nil {
		return nil
	}
	return response.StatusCode
}

func waitForWebSocketTest(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}
