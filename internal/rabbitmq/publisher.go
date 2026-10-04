package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/metrics"
)

const maxPublisherConcurrency = 64

var (
	ErrPublisherClosed             = errors.New("rabbitmq publisher is closed")
	ErrInvalidPublisherConcurrency = errors.New("rabbitmq publisher concurrency must be between 1 and 64")
	ErrPublishInterrupted          = errors.New("rabbitmq publication was interrupted")
	ErrPublishNacked               = errors.New("rabbitmq publication was negatively acknowledged")
	ErrPublishUnroutable           = errors.New("rabbitmq publication was returned as unroutable")
)

type UnroutableError struct {
	Exchange   string
	RoutingKey string
	ReplyCode  uint16
	ReplyText  string
}

func (e *UnroutableError) Error() string {
	return fmt.Sprintf(
		"%s: exchange=%q routing_key=%q reply_code=%d reply_text=%q",
		ErrPublishUnroutable,
		e.Exchange,
		e.RoutingKey,
		e.ReplyCode,
		e.ReplyText,
	)
}

func (e *UnroutableError) Unwrap() error {
	return ErrPublishUnroutable
}

// Publisher uses a bounded set of independent AMQP Channels. Each lane allows
// one in-flight publication so confirms and mandatory returns remain isolated,
// while separate lanes can publish concurrently.
type Publisher struct {
	mu        sync.Mutex
	lanes     []publishLane
	available chan publishLane
	active    sync.WaitGroup
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

type publishLane interface {
	publish(context.Context, messaging.Event, []byte) error
	close() error
}

func NewPublisher(ctx context.Context, client *Client, concurrency int) (*Publisher, error) {
	if concurrency <= 0 || concurrency > maxPublisherConcurrency {
		return nil, ErrInvalidPublisherConcurrency
	}
	publisher := &Publisher{
		lanes:     make([]publishLane, 0, concurrency),
		available: make(chan publishLane, concurrency),
	}
	for range concurrency {
		lane := &publisherLane{client: client}
		if err := lane.ensureChannel(ctx); err != nil {
			for _, initialized := range publisher.lanes {
				_ = initialized.close()
			}
			return nil, err
		}
		publisher.lanes = append(publisher.lanes, lane)
		publisher.available <- lane
	}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, event messaging.Event) (publishErr error) {
	started := time.Now()
	eventType := string(event.EventType)
	if !event.EventType.IsSupported() {
		eventType = "unknown"
	}
	defer func() {
		result := publishResult(publishErr)
		metrics.RabbitMQPublishTotal.WithLabelValues(eventType, result).Inc()
		metrics.RabbitMQPublishDuration.WithLabelValues(eventType, result).Observe(time.Since(started).Seconds())
	}()

	body, err := event.Marshal()
	if err != nil {
		return err
	}
	lane, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(lane)
	return lane.publish(ctx, event, body)
}

func (p *Publisher) acquire(ctx context.Context) (publishLane, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPublisherClosed
	}
	available := p.available
	p.active.Add(1)
	p.mu.Unlock()

	select {
	case lane := <-available:
		return lane, nil
	case <-ctx.Done():
		p.active.Done()
		return nil, ctx.Err()
	}
}

func (p *Publisher) release(lane publishLane) {
	p.available <- lane
	p.active.Done()
}

func (p *Publisher) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.active.Wait()
		for _, lane := range p.lanes {
			p.closeErr = errors.Join(p.closeErr, lane.close())
		}
	})
	return p.closeErr
}

func publishResult(err error) string {
	switch {
	case err == nil:
		return "confirmed"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrPublisherClosed):
		return "closed"
	case errors.Is(err, ErrPublishUnroutable):
		return "unroutable"
	case errors.Is(err, ErrPublishNacked):
		return "nacked"
	case errors.Is(err, ErrPublishInterrupted):
		return "interrupted"
	case errors.Is(err, messaging.ErrInvalidEvent):
		return "invalid"
	default:
		return "error"
	}
}
