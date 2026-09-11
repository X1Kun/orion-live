package websocket

import (
	"errors"
	"sync"
)

var (
	ErrInvalidClientQueueCapacity = errors.New("client send queue capacity must be positive")
	ErrClientClosed               = errors.New("websocket client is closed")
	ErrClientAlreadyJoined        = errors.New("websocket client is already joined to a room")
)

// Client is a connection-independent outbound message queue owned by a Room.
type Client struct {
	mu         sync.Mutex
	joined     bool
	outboundCh chan []byte
	done       chan struct{}
	closed     bool
}

func (c *Client) claimRoom() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClientClosed
	}
	if c.joined {
		return ErrClientAlreadyJoined
	}
	c.joined = true
	return nil
}

func (c *Client) rollbackRoomClaim() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.joined = false
	}
}

func NewClient(sendQueueCapacity int) (*Client, error) {
	if sendQueueCapacity <= 0 {
		return nil, ErrInvalidClientQueueCapacity
	}
	return &Client{
		outboundCh: make(chan []byte, sendQueueCapacity),
		done:       make(chan struct{}),
	}, nil
}

// Outbound returns the queue consumed by the future WebSocket writer.
func (c *Client) Outbound() <-chan []byte {
	return c.outboundCh
}

// Done is closed when the client must stop.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

func (c *Client) enqueueOutbound(message []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}

	copyOfMessage := append([]byte(nil), message...)
	select {
	case c.outboundCh <- copyOfMessage:
		return true
	default:
		return false
	}
}

func (c *Client) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.joined = false
	close(c.done)
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
