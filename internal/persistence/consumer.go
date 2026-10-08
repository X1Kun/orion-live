package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/metrics"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	consumerName      = "chat-persistence-v1"
	reconnectMinDelay = 100 * time.Millisecond
	reconnectMaxDelay = 5 * time.Second
)

var ErrConsumerNotReady = errors.New("persistence consumer is not ready")

type Consumer struct {
	client     *rabbitclient.Client
	repository repository.ChatPersistenceRepository
	config     config.Persistence

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}
	ready  atomic.Bool
}

type consumerSession struct {
	channel         *amqp.Channel
	deliveriesCh    <-chan amqp.Delivery
	channelClosedCh <-chan *amqp.Error
}

func StartConsumer(
	ctx context.Context,
	client *rabbitclient.Client,
	repository repository.ChatPersistenceRepository,
	cfg config.Persistence,
) (*Consumer, error) {
	runCtx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client, repository: repository, config: cfg,
		ctx: runCtx, cancel: cancel, doneCh: make(chan struct{}),
	}
	session, err := consumer.openSession(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	consumer.setReady(true)
	go consumer.run(session)
	return consumer, nil
}

func (c *Consumer) Ready() error {
	if !c.ready.Load() {
		return ErrConsumerNotReady
	}
	return nil
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.cancel()
	select {
	case <-c.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) run(current *consumerSession) {
	defer close(c.doneCh)
	for {
		consumeErr := c.consumeSession(current)
		_ = current.channel.Close()
		c.setReady(false)
		if c.ctx.Err() != nil {
			return
		}
		logger.Log.WithError(consumeErr).Warn("persistence consumer interrupted")
		next, err := c.recoverSession()
		if err != nil {
			return
		}
		current = next
		c.setReady(true)
		logger.Log.Info("persistence consumer recovered")
	}
}

func (c *Consumer) consumeSession(session *consumerSession) error {
	sessionCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	workerErrCh := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(c.config.Concurrency)
	for range c.config.Concurrency {
		go func() {
			defer workers.Done()
			if err := c.consumeDeliveries(sessionCtx, session.deliveriesCh); err != nil {
				select {
				case workerErrCh <- err:
					cancel()
				default:
				}
			}
		}()
	}

	var consumeErr error
	select {
	case <-c.ctx.Done():
		consumeErr = c.ctx.Err()
	case amqpErr, ok := <-session.channelClosedCh:
		if ok && amqpErr != nil {
			consumeErr = fmt.Errorf("persistence channel closed: %w", amqpErr)
		} else {
			consumeErr = ErrConsumerNotReady
		}
	case consumeErr = <-workerErrCh:
	}
	cancel()
	workers.Wait()
	return consumeErr
}

func (c *Consumer) consumeDeliveries(ctx context.Context, deliveriesCh <-chan amqp.Delivery) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveriesCh:
			if !ok {
				return ErrConsumerNotReady
			}
			if err := c.handleDelivery(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) handleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	started := time.Now()
	metricResult := "error"
	defer func() {
		metrics.PersistenceProcessingDuration.WithLabelValues(metricResult).Observe(time.Since(started).Seconds())
	}()
	event, payload, err := decodeChatEvent(delivery.Body)
	if err != nil {
		metricResult = "malformed"
		return deadLetter(delivery, "malformed")
	}
	processCtx, cancel := context.WithTimeout(ctx, c.config.ProcessingTimeout)
	result, err := c.repository.Persist(processCtx, consumerName, event, payload)
	cancel()
	if err != nil {
		metricResult = "retry"
		logger.Log.WithError(err).WithField("event_id", event.EventID).Warn("retry Chat persistence")
		if err := delivery.Reject(true); err != nil {
			metricResult = "retry_reject_error"
			return fmt.Errorf("reject Chat event for retry: %w", err)
		}
		metrics.PersistenceEventsTotal.WithLabelValues("retry").Inc()
		return nil
	}
	if result == repository.ChatConflictingMessage {
		metricResult = string(result)
		metrics.ChatConflictsTotal.Inc()
		logger.Log.WithFields(map[string]any{
			"event_id": event.EventID, "live_session_id": event.LiveSessionID,
			"user_id": event.UserID, "message_id": payload.MessageID,
		}).Warn("conflicting Chat message")
		return deadLetter(delivery, string(result))
	}
	if err := delivery.Ack(false); err != nil {
		metricResult = "ack_error"
		return fmt.Errorf("ack persisted Chat event: %w", err)
	}
	metricResult = string(result)
	metrics.PersistenceEventsTotal.WithLabelValues(string(result)).Inc()
	if result == repository.ChatPersisted {
		lag := time.Since(payload.AcceptedAt.UTC()).Seconds()
		if lag < 0 {
			lag = 0
		}
		metrics.ChatPersistenceLag.Observe(lag)
	}
	return nil
}

func (c *Consumer) openSession(ctx context.Context) (*consumerSession, error) {
	channel, err := c.client.Channel(ctx)
	if err != nil {
		return nil, err
	}
	channelClosedCh := channel.NotifyClose(make(chan *amqp.Error, 1))
	if err := channel.Qos(c.config.Prefetch, 0, false); err != nil {
		_ = channel.Close()
		return nil, fmt.Errorf("configure persistence qos: %w", err)
	}
	deliveriesCh, err := channel.Consume(rabbitclient.PersistenceQueueName, "", false, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		return nil, fmt.Errorf("start persistence consumer: %w", err)
	}
	return &consumerSession{channel: channel, deliveriesCh: deliveriesCh, channelClosedCh: channelClosedCh}, nil
}

func (c *Consumer) recoverSession() (*consumerSession, error) {
	delay := reconnectMinDelay
	for {
		session, err := c.openSession(c.ctx)
		if err == nil {
			return session, nil
		}
		if c.ctx.Err() != nil {
			return nil, c.ctx.Err()
		}
		jittered := time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
		timer := time.NewTimer(jittered)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return nil, c.ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (c *Consumer) setReady(ready bool) {
	c.ready.Store(ready)
	if ready {
		metrics.PersistenceConsumerReady.Set(1)
	} else {
		metrics.PersistenceConsumerReady.Set(0)
	}
}

func decodeChatEvent(body []byte) (messaging.Event, messaging.ChatMessageAcceptedPayload, error) {
	var event messaging.Event
	if err := json.Unmarshal(body, &event); err != nil {
		return messaging.Event{}, messaging.ChatMessageAcceptedPayload{}, err
	}
	if err := event.Validate(); err != nil || event.EventType != messaging.EventTypeChatMessageAccepted {
		return messaging.Event{}, messaging.ChatMessageAcceptedPayload{}, messaging.ErrInvalidEvent
	}
	var payload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return messaging.Event{}, messaging.ChatMessageAcceptedPayload{}, err
	}
	if err := payload.Validate(); err != nil || !payload.AcceptedAt.Equal(event.OccurredAt) {
		return messaging.Event{}, messaging.ChatMessageAcceptedPayload{}, messaging.ErrInvalidChatPayload
	}
	return event, payload, nil
}

func deadLetter(delivery amqp.Delivery, result string) error {
	if err := delivery.Reject(false); err != nil {
		return fmt.Errorf("dead-letter Chat event: %w", err)
	}
	metrics.PersistenceEventsTotal.WithLabelValues(result).Inc()
	return nil
}
