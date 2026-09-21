package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/X1Kun/orion-live/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrPublisherClosed   = errors.New("rabbitmq publisher is closed")
	ErrPublishNacked     = errors.New("rabbitmq publication was negatively acknowledged")
	ErrPublishUnroutable = errors.New("rabbitmq publication was returned as unroutable")
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

type Publisher struct {
	client *Client

	mu      sync.Mutex
	channel *amqp.Channel
	returns <-chan amqp.Return
	closed  <-chan *amqp.Error
	stopped bool
}

func NewPublisher(ctx context.Context, client *Client) (*Publisher, error) {
	publisher := &Publisher{client: client}
	publisher.mu.Lock()
	err := publisher.openChannelLocked(ctx)
	publisher.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, event messaging.Event) error {
	body, err := event.Marshal()
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return ErrPublisherClosed
	}
	if err := p.openChannelLocked(ctx); err != nil {
		return err
	}

	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		messaging.ExchangeName,
		event.RoutingKey(),
		true,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"schema_version": int32(event.SchemaVersion),
			},
			ContentType:   "application/json",
			DeliveryMode:  amqp.Persistent,
			CorrelationId: event.CorrelationID,
			MessageId:     event.EventID,
			Timestamp:     event.OccurredAt,
			Type:          string(event.EventType),
			AppId:         "orion-live",
			Body:          body,
		},
	)
	if err != nil {
		p.invalidateChannelLocked()
		return fmt.Errorf("publish interaction event: %w", err)
	}
	if confirmation == nil {
		p.invalidateChannelLocked()
		return errors.New("rabbitmq publisher confirm mode is not enabled")
	}

	for {
		select {
		case returned, ok := <-p.returns:
			if !ok {
				p.invalidateChannelLocked()
				return ErrPublisherClosed
			}
			return newUnroutableError(returned)
		case <-confirmation.Done():
			if !confirmation.Acked() {
				return ErrPublishNacked
			}
			select {
			case returned, ok := <-p.returns:
				if !ok {
					p.invalidateChannelLocked()
					return ErrPublisherClosed
				}
				return newUnroutableError(returned)
			default:
				return nil
			}
		case amqpErr, ok := <-p.closed:
			p.invalidateChannelLocked()
			if ok && amqpErr != nil {
				return fmt.Errorf("%w: %v", ErrPublisherClosed, amqpErr)
			}
			return ErrPublisherClosed
		case <-ctx.Done():
			p.invalidateChannelLocked()
			return ctx.Err()
		}
	}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	p.stopped = true
	if p.channel == nil || p.channel.IsClosed() {
		return nil
	}
	err := p.channel.Close()
	p.channel = nil
	return err
}

func (p *Publisher) openChannelLocked(ctx context.Context) error {
	if p.stopped {
		return ErrPublisherClosed
	}
	if p.channel != nil && !p.channel.IsClosed() {
		return nil
	}
	channel, err := p.client.Channel(ctx)
	if err != nil {
		return err
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.channel = channel
	p.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	p.closed = channel.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

func (p *Publisher) invalidateChannelLocked() {
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	p.channel = nil
	p.returns = nil
	p.closed = nil
}

func newUnroutableError(returned amqp.Return) error {
	return &UnroutableError{
		Exchange:   returned.Exchange,
		RoutingKey: returned.RoutingKey,
		ReplyCode:  returned.ReplyCode,
		ReplyText:  returned.ReplyText,
	}
}
