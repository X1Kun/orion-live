package config

import (
	"testing"
	"time"
)

func TestLoadWebSocketDefaults(t *testing.T) {
	for _, name := range []string{
		"WEBSOCKET_HANDSHAKE_TIMEOUT",
		"WEBSOCKET_WRITE_TIMEOUT",
		"WEBSOCKET_PONG_TIMEOUT",
		"WEBSOCKET_PING_INTERVAL",
		"WEBSOCKET_READ_LIMIT_BYTES",
		"WEBSOCKET_CLIENT_SEND_QUEUE_CAPACITY",
		"WEBSOCKET_ROOM_BROADCAST_QUEUE_CAPACITY",
		"WEBSOCKET_MAX_CONNECTIONS",
		"WEBSOCKET_MAX_CONNECTIONS_PER_USER",
	} {
		t.Setenv(name, "")
	}

	cfg, err := loadWebSocket()
	if err != nil {
		t.Fatalf("loadWebSocket() error = %v", err)
	}
	want := WebSocket{
		HandshakeTimeout:           5 * time.Second,
		WriteTimeout:               10 * time.Second,
		PongTimeout:                60 * time.Second,
		PingInterval:               45 * time.Second,
		ReadLimitBytes:             4096,
		ClientSendQueueCapacity:    64,
		RoomBroadcastQueueCapacity: 256,
		MaxConnections:             1000,
		MaxConnectionsPerUser:      3,
	}
	if cfg != want {
		t.Fatalf("loadWebSocket() = %#v, want %#v", cfg, want)
	}
}

func TestWebSocketValidate(t *testing.T) {
	valid := WebSocket{
		HandshakeTimeout:           time.Second,
		WriteTimeout:               time.Second,
		PongTimeout:                time.Minute,
		PingInterval:               45 * time.Second,
		ReadLimitBytes:             4096,
		ClientSendQueueCapacity:    64,
		RoomBroadcastQueueCapacity: 256,
		MaxConnections:             1000,
		MaxConnectionsPerUser:      3,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid WebSocket configuration rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*WebSocket)
	}{
		{name: "non-positive timeout", mutate: func(c *WebSocket) { c.WriteTimeout = 0 }},
		{name: "ping not shorter than pong", mutate: func(c *WebSocket) { c.PingInterval = c.PongTimeout }},
		{name: "read limit above maximum", mutate: func(c *WebSocket) { c.ReadLimitBytes = maxWebSocketReadLimitBytes + 1 }},
		{name: "non-positive client queue", mutate: func(c *WebSocket) { c.ClientSendQueueCapacity = 0 }},
		{name: "non-positive room queue", mutate: func(c *WebSocket) { c.RoomBroadcastQueueCapacity = 0 }},
		{name: "non-positive global limit", mutate: func(c *WebSocket) { c.MaxConnections = 0 }},
		{name: "per-user limit above global", mutate: func(c *WebSocket) { c.MaxConnectionsPerUser = c.MaxConnections + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("invalid WebSocket configuration was accepted")
			}
		})
	}
}
