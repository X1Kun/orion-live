package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/metrics"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/X1Kun/orion-live/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	reconnectMinDelay = 100 * time.Millisecond
	reconnectMaxDelay = 5 * time.Second
	unknownEventType  = "unknown"
)

var (
	ErrInvalidPrefetch    = errors.New("realtime prefetch must be positive")
	ErrSubscriberNotReady = errors.New("realtime subscriber is not ready")
)

type Subscriber struct {
	client   *rabbitclient.Client
	hub      *roomhub.Hub
	prefetch int

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

func Start(ctx context.Context, client *rabbitclient.Client, hub *roomhub.Hub, prefetch int) (*Subscriber, error) {
	if prefetch <= 0 {
		return nil, ErrInvalidPrefetch
	}
	runCtx, cancel := context.WithCancel(context.Background())
	subscriber := &Subscriber{
		client:   client,
		hub:      hub,
		prefetch: prefetch,
		ctx:      runCtx,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
	}
	session, err := subscriber.openSession(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	subscriber.setReady(true)
	go subscriber.run(session)
	return subscriber, nil
}

func (s *Subscriber) Ready() error {
	if !s.ready.Load() {
		return ErrSubscriberNotReady
	}
	return nil
}

func (s *Subscriber) Shutdown(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Subscriber) run(current *consumerSession) {
	defer close(s.doneCh)
	for {
		consumeErr := s.consumeSession(current)
		_ = current.channel.Close()
		s.setReady(false)
		if s.ctx.Err() != nil {
			return
		}
		logger.Log.WithError(consumeErr).Warn("realtime subscriber interrupted")

		next, err := s.recoverSession()
		if err != nil {
			return
		}
		current = next
		s.setReady(true)
		logger.Log.Info("realtime subscriber recovered")
	}
}

func (s *Subscriber) consumeSession(session *consumerSession) error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case amqpErr, ok := <-session.channelClosedCh:
			if ok && amqpErr != nil {
				return fmt.Errorf("realtime channel closed: %w", amqpErr)
			}
			return ErrSubscriberNotReady
		case delivery, ok := <-session.deliveriesCh:
			if !ok {
				return ErrSubscriberNotReady
			}
			if err := s.handleDelivery(delivery); err != nil {
				return err
			}
		}
	}
}

func (s *Subscriber) handleDelivery(delivery amqp.Delivery) error {
	var event messaging.Event
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		return acknowledgeRealtime(delivery, unknownEventType, "malformed")
	}
	eventType := realtimeEventTypeLabel(event.EventType)
	if eventType == unknownEventType {
		return acknowledgeRealtime(delivery, eventType, "unsupported_type")
	}
	if err := event.Validate(); err != nil {
		return acknowledgeRealtime(delivery, eventType, "malformed")
	}

	// Forward the validated original envelope to preserve versioned unknown fields
	// and avoid a second JSON encoding on the realtime path.
	present, err := s.hub.BroadcastIfPresent(event.LiveSessionID, delivery.Body)
	switch {
	case errors.Is(err, roomhub.ErrRoomQueueFull):
		return acknowledgeRealtime(delivery, eventType, "room_queue_full")
	case errors.Is(err, roomhub.ErrHubClosed):
		return acknowledgeRealtime(delivery, eventType, "hub_closed")
	case err != nil:
		return acknowledgeRealtime(delivery, eventType, "hub_error")
	case !present:
		return acknowledgeRealtime(delivery, eventType, "no_room")
	default:
		return acknowledgeRealtime(delivery, eventType, "delivered")
	}
}

func (s *Subscriber) openSession(ctx context.Context) (*consumerSession, error) {
	channel, err := s.client.Channel(ctx)
	if err != nil {
		return nil, err
	}
	channelClosedCh := channel.NotifyClose(make(chan *amqp.Error, 1))
	fail := func(err error) (*consumerSession, error) {
		_ = channel.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare("", false, true, true, false, amqp.Table{"x-queue-type": "classic"})
	if err != nil {
		return fail(fmt.Errorf("declare realtime queue: %w", err))
	}
	for _, routingKey := range []string{
		string(messaging.EventTypeLiveSessionEnded),
		string(messaging.EventTypeChatMessageAccepted),
	} {
		if err := channel.QueueBind(queue.Name, routingKey, rabbitclient.InteractionExchangeName, false, nil); err != nil {
			return fail(fmt.Errorf("bind realtime queue to %q: %w", routingKey, err))
		}
	}
	if err := channel.Qos(s.prefetch, 0, false); err != nil {
		return fail(fmt.Errorf("configure realtime qos: %w", err))
	}
	deliveriesCh, err := channel.Consume(queue.Name, "", false, true, false, false, nil)
	if err != nil {
		return fail(fmt.Errorf("start realtime consumer: %w", err))
	}
	return &consumerSession{channel: channel, deliveriesCh: deliveriesCh, channelClosedCh: channelClosedCh}, nil
}

func (s *Subscriber) recoverSession() (*consumerSession, error) {
	delay := reconnectMinDelay
	for {
		session, err := s.openSession(s.ctx)
		if err == nil {
			return session, nil
		}
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		logger.Log.WithError(err).WithField("retry_delay", delay).Debug("recreate realtime subscriber")

		jittered := time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
		timer := time.NewTimer(jittered)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return nil, s.ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (s *Subscriber) setReady(ready bool) {
	s.ready.Store(ready)
	if ready {
		metrics.RealtimeSubscriberReady.Set(1)
	} else {
		metrics.RealtimeSubscriberReady.Set(0)
	}
}

func acknowledgeRealtime(delivery amqp.Delivery, eventType, result string) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("ack realtime event: %w", err)
	}
	metrics.RealtimeEventsTotal.WithLabelValues(eventType, result).Inc()
	return nil
}

func realtimeEventTypeLabel(eventType messaging.EventType) string {
	if !eventType.IsSupported() {
		return unknownEventType
	}
	return string(eventType)
}
