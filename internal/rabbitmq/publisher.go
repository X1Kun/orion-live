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
	ErrPublisherClosed    = errors.New("rabbitmq publisher is closed")
	ErrPublishInterrupted = errors.New("rabbitmq publication was interrupted")
	ErrPublishNacked      = errors.New("rabbitmq publication was negatively acknowledged")
	ErrPublishUnroutable  = errors.New("rabbitmq publication was returned as unroutable")
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

	mu              sync.Mutex
	channel         *amqp.Channel
	returnsCh       <-chan amqp.Return
	channelClosedCh <-chan *amqp.Error
	closed          bool
}

func NewPublisher(ctx context.Context, client *Client) (*Publisher, error) {
	publisher := &Publisher{client: client}
	publisher.mu.Lock()
	err := publisher.ensureChannelLocked(ctx)
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
	if p.closed {
		return ErrPublisherClosed
	}
	if err := p.ensureChannelLocked(ctx); err != nil {
		return err
	}

	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		InteractionExchangeName,
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrPublishInterrupted, err)
	}
	if confirmation == nil {
		p.invalidateChannelLocked()
		return fmt.Errorf("%w: publisher confirm mode is not enabled", ErrPublishInterrupted)
	}
	return p.awaitPublishResult(ctx, confirmation)
}

func (p *Publisher) awaitPublishResult(ctx context.Context, confirmation *amqp.DeferredConfirmation) error {
	select {
	case <-ctx.Done():
		p.invalidateChannelLocked()
		return ctx.Err()
	case amqpErr, ok := <-p.channelClosedCh:
		p.invalidateChannelLocked()
		if ok && amqpErr != nil {
			return fmt.Errorf("%w: %v", ErrPublishInterrupted, amqpErr)
		}
		return ErrPublishInterrupted
	case returned, ok := <-p.returnsCh:
		if !ok {
			p.invalidateChannelLocked()
			return ErrPublishInterrupted
		}
		return newUnroutableError(returned)
	case <-confirmation.Done():
		if !confirmation.Acked() {
			return ErrPublishNacked
		}
		return p.pendingReturnOrNil()
	}
}

func (p *Publisher) pendingReturnOrNil() error {
	select {
	case returned, ok := <-p.returnsCh:
		if !ok {
			p.invalidateChannelLocked()
			return ErrPublishInterrupted
		}
		return newUnroutableError(returned)
	default:
		return nil
	}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.channel == nil || p.channel.IsClosed() {
		return nil
	}
	err := p.channel.Close()
	p.channel = nil
	return err
}

func (p *Publisher) ensureChannelLocked(ctx context.Context) error {
	if p.closed {
		return ErrPublisherClosed
	}
	if p.channel != nil && !p.channel.IsClosed() {
		return nil
	}
	channel, err := p.client.Channel(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrPublishInterrupted, err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		return fmt.Errorf("%w: enable publisher confirms: %v", ErrPublishInterrupted, err)
	}
	p.channel = channel
	p.returnsCh = channel.NotifyReturn(make(chan amqp.Return, 1))
	p.channelClosedCh = channel.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

func (p *Publisher) invalidateChannelLocked() {
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
	}
	p.channel = nil
	p.returnsCh = nil
	p.channelClosedCh = nil
}

func newUnroutableError(returned amqp.Return) error {
	return &UnroutableError{
		Exchange:   returned.Exchange,
		RoutingKey: returned.RoutingKey,
		ReplyCode:  returned.ReplyCode,
		ReplyText:  returned.ReplyText,
	}
}
