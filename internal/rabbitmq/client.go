package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	reconnectMinDelay    = 100 * time.Millisecond
	reconnectMaxDelay    = 5 * time.Second
	reconnectDialTimeout = 5 * time.Second
)

var (
	ErrClientClosed = errors.New("rabbitmq client is closed")
	ErrUnavailable  = errors.New("rabbitmq connection is unavailable")
)

type Client struct {
	cfg config.RabbitMQ

	mu      sync.RWMutex
	conn    *amqp.Connection
	readyCh chan struct{}
	closed  bool

	shutdownCh       chan struct{}
	supervisorDoneCh chan struct{}
	closeOnce        sync.Once
}

func Open(ctx context.Context, cfg config.RabbitMQ) (*Client, error) {
	conn, err := dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	readyCh := make(chan struct{})
	close(readyCh)
	client := &Client{
		cfg:              cfg,
		conn:             conn,
		readyCh:          readyCh,
		shutdownCh:       make(chan struct{}),
		supervisorDoneCh: make(chan struct{}),
	}
	go client.supervise(conn)
	return client, nil
}

func (c *Client) Connection(ctx context.Context) (*amqp.Connection, error) {
	for {
		c.mu.RLock()
		conn := c.conn
		readyCh := c.readyCh
		closed := c.closed
		c.mu.RUnlock()
		if closed {
			return nil, ErrClientClosed
		}
		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.shutdownCh:
			return nil, ErrClientClosed
		case <-readyCh:
		}
	}
}

func (c *Client) Channel(ctx context.Context) (*amqp.Channel, error) {
	conn, err := c.Connection(ctx)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}
	return channel, nil
}

func (c *Client) Ready() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClientClosed
	}
	if c.conn == nil || c.conn.IsClosed() {
		return ErrUnavailable
	}
	return nil
}

func (c *Client) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		conn := c.conn
		c.mu.Unlock()
		close(c.shutdownCh)
		if conn != nil && !conn.IsClosed() {
			closeErr = conn.Close()
		}
		<-c.supervisorDoneCh
	})
	return closeErr
}

func (c *Client) supervise(initial *amqp.Connection) {
	defer close(c.supervisorDoneCh)

	current := initial
	for {
		connectionClosedCh := current.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-c.shutdownCh:
			c.clearConnection(current)
			return
		case <-connectionClosedCh:
			c.clearConnection(current)
		}

		next, err := c.recoverConnection()
		if err != nil {
			return
		}
		current = next
	}
}

func (c *Client) recoverConnection() (*amqp.Connection, error) {
	delay := reconnectMinDelay
	for {
		select {
		case <-c.shutdownCh:
			return nil, ErrClientClosed
		default:
		}

		dialCtx, cancel := context.WithTimeout(context.Background(), reconnectDialTimeout)
		next, err := dial(dialCtx, c.cfg)
		cancel()
		if err == nil {
			if c.installConnection(next) {
				return next, nil
			}
			_ = next.Close()
			return nil, ErrClientClosed
		}

		jittered := time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
		timer := time.NewTimer(jittered)
		select {
		case <-c.shutdownCh:
			timer.Stop()
			return nil, ErrClientClosed
		case <-timer.C:
		}
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (c *Client) clearConnection(expected *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != expected {
		return
	}
	c.conn = nil
	if !c.closed {
		c.readyCh = make(chan struct{})
	}
}

func (c *Client) installConnection(conn *amqp.Connection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.conn = conn
	close(c.readyCh)
	return true
}

func dial(ctx context.Context, cfg config.RabbitMQ) (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(cfg.URL(), amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		// Recovery intentionally remains nil. Orion owns connection recovery here,
		// while Publishers and Consumers own channel recreation.
		Dial: func(network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq connection: %w", err)
	}
	return conn, nil
}
