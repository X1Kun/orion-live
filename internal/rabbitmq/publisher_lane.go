package rabbitmq

import (
	"context"
	"fmt"

	"github.com/X1Kun/orion-live/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type publisherLane struct {
	client *Client

	channel         *amqp.Channel
	returnsCh       <-chan amqp.Return
	channelClosedCh <-chan *amqp.Error
	closed          bool
}

func (p *publisherLane) publish(ctx context.Context, event messaging.Event, body []byte) error {
	if p.closed {
		return ErrPublisherClosed
	}
	if err := p.ensureChannel(ctx); err != nil {
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
		p.invalidateChannel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrPublishInterrupted, err)
	}
	if confirmation == nil {
		p.invalidateChannel()
		return fmt.Errorf("%w: publisher confirm mode is not enabled", ErrPublishInterrupted)
	}
	return p.awaitPublishResult(ctx, confirmation)
}

func (p *publisherLane) awaitPublishResult(ctx context.Context, confirmation *amqp.DeferredConfirmation) error {
	select {
	case <-ctx.Done():
		p.invalidateChannel()
		return ctx.Err()
	case amqpErr, ok := <-p.channelClosedCh:
		p.invalidateChannel()
		if ok && amqpErr != nil {
			return fmt.Errorf("%w: %v", ErrPublishInterrupted, amqpErr)
		}
		return ErrPublishInterrupted
	case returned, ok := <-p.returnsCh:
		if !ok {
			p.invalidateChannel()
			return ErrPublishInterrupted
		}
		return newUnroutableError(returned)
	case <-confirmation.Done():
		if !confirmation.Acked() {
			if p.channel == nil || p.channel.IsClosed() {
				p.invalidateChannel()
				return fmt.Errorf("%w: %v", ErrPublishInterrupted, ErrPublishNacked)
			}
			return ErrPublishNacked
		}
		return p.pendingReturnOrNil()
	}
}

func (p *publisherLane) pendingReturnOrNil() error {
	select {
	case returned, ok := <-p.returnsCh:
		if !ok {
			p.invalidateChannel()
			return ErrPublishInterrupted
		}
		return newUnroutableError(returned)
	default:
		return nil
	}
}

func (p *publisherLane) close() error {
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

func (p *publisherLane) ensureChannel(ctx context.Context) error {
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

func (p *publisherLane) invalidateChannel() {
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
