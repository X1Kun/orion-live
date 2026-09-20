package config

import (
	"errors"
	"time"
)

const maxWebSocketReadLimitBytes = 1 << 20

type WebSocket struct {
	HandshakeTimeout           time.Duration
	WriteTimeout               time.Duration
	PongTimeout                time.Duration
	PingInterval               time.Duration
	ReadLimitBytes             int64
	ClientSendQueueCapacity    int
	RoomBroadcastQueueCapacity int
	MaxConnections             int
	MaxConnectionsPerUser      int
}

func loadWebSocket() (WebSocket, error) {
	handshakeTimeout, err := envDuration("WEBSOCKET_HANDSHAKE_TIMEOUT", 5*time.Second)
	if err != nil {
		return WebSocket{}, err
	}
	writeTimeout, err := envDuration("WEBSOCKET_WRITE_TIMEOUT", 10*time.Second)
	if err != nil {
		return WebSocket{}, err
	}
	pongTimeout, err := envDuration("WEBSOCKET_PONG_TIMEOUT", 60*time.Second)
	if err != nil {
		return WebSocket{}, err
	}
	pingInterval, err := envDuration("WEBSOCKET_PING_INTERVAL", 45*time.Second)
	if err != nil {
		return WebSocket{}, err
	}
	readLimitBytes, err := envInt("WEBSOCKET_READ_LIMIT_BYTES", 4096)
	if err != nil {
		return WebSocket{}, err
	}
	clientSendQueueCapacity, err := envInt("WEBSOCKET_CLIENT_SEND_QUEUE_CAPACITY", 64)
	if err != nil {
		return WebSocket{}, err
	}
	roomBroadcastQueueCapacity, err := envInt("WEBSOCKET_ROOM_BROADCAST_QUEUE_CAPACITY", 256)
	if err != nil {
		return WebSocket{}, err
	}
	maxConnections, err := envInt("WEBSOCKET_MAX_CONNECTIONS", 1000)
	if err != nil {
		return WebSocket{}, err
	}
	maxConnectionsPerUser, err := envInt("WEBSOCKET_MAX_CONNECTIONS_PER_USER", 3)
	if err != nil {
		return WebSocket{}, err
	}

	return WebSocket{
		HandshakeTimeout:           handshakeTimeout,
		WriteTimeout:               writeTimeout,
		PongTimeout:                pongTimeout,
		PingInterval:               pingInterval,
		ReadLimitBytes:             int64(readLimitBytes),
		ClientSendQueueCapacity:    clientSendQueueCapacity,
		RoomBroadcastQueueCapacity: roomBroadcastQueueCapacity,
		MaxConnections:             maxConnections,
		MaxConnectionsPerUser:      maxConnectionsPerUser,
	}, nil
}

func (c WebSocket) validate() error {
	if c.HandshakeTimeout <= 0 || c.WriteTimeout <= 0 || c.PongTimeout <= 0 || c.PingInterval <= 0 {
		return errors.New("WebSocket timeouts must be positive")
	}
	if c.PingInterval >= c.PongTimeout {
		return errors.New("WEBSOCKET_PING_INTERVAL must be shorter than WEBSOCKET_PONG_TIMEOUT")
	}
	if c.ReadLimitBytes <= 0 || c.ReadLimitBytes > maxWebSocketReadLimitBytes {
		return errors.New("WEBSOCKET_READ_LIMIT_BYTES must be between 1 and 1048576")
	}
	if c.ClientSendQueueCapacity <= 0 {
		return errors.New("WEBSOCKET_CLIENT_SEND_QUEUE_CAPACITY must be positive")
	}
	if c.RoomBroadcastQueueCapacity <= 0 {
		return errors.New("WEBSOCKET_ROOM_BROADCAST_QUEUE_CAPACITY must be positive")
	}
	if c.MaxConnections <= 0 {
		return errors.New("WEBSOCKET_MAX_CONNECTIONS must be positive")
	}
	if c.MaxConnectionsPerUser <= 0 || c.MaxConnectionsPerUser > c.MaxConnections {
		return errors.New("WEBSOCKET_MAX_CONNECTIONS_PER_USER must be between 1 and WEBSOCKET_MAX_CONNECTIONS")
	}
	return nil
}
