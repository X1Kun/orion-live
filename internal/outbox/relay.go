package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/metrics"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/pkg/logger"
)

type EventPublisher interface {
	Publish(context.Context, messaging.Event) error
}

type Relay struct {
	repository repository.OutboxRepository
	publisher  EventPublisher
	owner      string
	config     config.Outbox
	ctx        context.Context
	cancel     context.CancelFunc
	doneCh     chan struct{}
}

func StartRelay(repository repository.OutboxRepository, publisher EventPublisher, owner string, cfg config.Outbox) *Relay {
	ctx, cancel := context.WithCancel(context.Background())
	relay := &Relay{repository: repository, publisher: publisher, owner: owner, config: cfg, ctx: ctx, cancel: cancel, doneCh: make(chan struct{})}
	go relay.run()
	return relay
}

func (r *Relay) Shutdown(ctx context.Context) error {
	r.cancel()
	select {
	case <-r.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Relay) run() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()
	for {
		r.processBatch()
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) processBatch() {
	events, err := r.repository.ClaimBatch(r.ctx, r.owner, r.config.BatchSize, r.config.LeaseDuration)
	if err != nil {
		if r.ctx.Err() == nil {
			logger.Log.WithError(err).Error("claim Outbox events")
		}
		return
	}
	for i := range events {
		if r.ctx.Err() != nil {
			return
		}
		r.process(events[i])
	}
}

func (r *Relay) process(record model.OutboxEvent) {
	if record.ClaimToken == nil {
		return
	}
	var event messaging.Event
	if err := json.Unmarshal(record.Payload, &event); err != nil || event.Validate() != nil {
		r.markFailed(record, errors.New("invalid Outbox event payload"))
		return
	}
	publishCtx, cancel := context.WithTimeout(r.ctx, r.config.PublishTimeout)
	err := r.publisher.Publish(publishCtx, event)
	cancel()
	if err == nil {
		updated, updateErr := r.repository.MarkPublished(r.ctx, record.EventID, *record.ClaimToken)
		r.recordUpdate(event.EventType, "published", updated, updateErr)
		return
	}
	if int(record.AttemptCount) >= r.config.MaxAttempts {
		r.markFailed(record, err)
		return
	}
	updated, updateErr := r.repository.ScheduleRetry(r.ctx, record.EventID, *record.ClaimToken, r.retryDelay(record.AttemptCount), err)
	r.recordUpdate(event.EventType, "retry", updated, updateErr)
}

func (r *Relay) markFailed(record model.OutboxEvent, cause error) {
	updated, err := r.repository.MarkFailed(r.ctx, record.EventID, *record.ClaimToken, cause)
	r.recordUpdate(messaging.EventType(record.EventType), "failed", updated, err)
}

func (r *Relay) recordUpdate(eventType messaging.EventType, result string, updated bool, err error) {
	if err != nil {
		logger.Log.WithError(err).WithField("event_type", eventType).Error("update Outbox event")
		return
	}
	if !updated {
		metrics.OutboxFencingFailures.Inc()
		return
	}
	metrics.OutboxPublishTotal.WithLabelValues(string(eventType), result).Inc()
}

func (r *Relay) retryDelay(attempt uint) time.Duration {
	delay := r.config.RetryBaseDelay
	for current := uint(1); current < attempt && delay < r.config.RetryMaxDelay; current++ {
		delay *= 2
		if delay > r.config.RetryMaxDelay {
			return r.config.RetryMaxDelay
		}
	}
	return delay
}
