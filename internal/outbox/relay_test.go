package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
)

func TestProcessMarksPublished(t *testing.T) {
	repository := &outboxRepositoryFake{}
	relay := newOutboxTestRelay(repository, nil)
	relay.process(outboxTestRecord(t, 1))
	if repository.published != 1 || repository.retried != 0 || repository.failed != 0 {
		t.Fatalf("published=%d retried=%d failed=%d", repository.published, repository.retried, repository.failed)
	}
}

func TestProcessSchedulesRetry(t *testing.T) {
	repository := &outboxRepositoryFake{}
	want := errors.New("broker unavailable")
	relay := newOutboxTestRelay(repository, want)
	relay.process(outboxTestRecord(t, 1))
	if repository.retried != 1 || repository.retryCause != want || repository.retryDelay != time.Second {
		t.Fatalf("retry state = %#v", repository)
	}
}

func TestProcessMarksExhaustedAndInvalidEventsFailed(t *testing.T) {
	tests := []struct {
		name       string
		record     func(*testing.T) model.OutboxEvent
		publishErr error
	}{
		{name: "exhausted", record: func(t *testing.T) model.OutboxEvent { return outboxTestRecord(t, 3) }, publishErr: errors.New("nack")},
		{name: "invalid payload", record: func(*testing.T) model.OutboxEvent {
			token := "claim"
			return model.OutboxEvent{EventID: "invalid", EventType: string(messaging.EventTypeLiveSessionEnded), ClaimToken: &token, Payload: []byte("{")}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &outboxRepositoryFake{}
			relay := newOutboxTestRelay(repository, tt.publishErr)
			relay.process(tt.record(t))
			if repository.failed != 1 {
				t.Fatalf("failed updates = %d, want 1", repository.failed)
			}
		})
	}
}

func newOutboxTestRelay(repository *outboxRepositoryFake, publishErr error) *Relay {
	return &Relay{
		repository: repository,
		publisher:  outboxEventPublisherFake{err: publishErr},
		owner:      "test",
		config: config.Outbox{
			PollInterval: time.Second, BatchSize: 1, LeaseDuration: time.Minute, MaxAttempts: 3,
			PublishTimeout: time.Second, RetryBaseDelay: time.Second, RetryMaxDelay: 4 * time.Second,
		},
		ctx: context.Background(),
	}
}

func outboxTestRecord(t *testing.T, attempts uint) model.OutboxEvent {
	t.Helper()
	event, err := messaging.NewLiveSessionEndedEvent(1, 42, time.Now().UTC(), "correlation")
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	body, err := event.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	token := "claim"
	return model.OutboxEvent{EventID: event.EventID, EventType: string(event.EventType), SchemaVersion: 1, Payload: body, ClaimToken: &token, AttemptCount: attempts}
}

type outboxEventPublisherFake struct{ err error }

func (p outboxEventPublisherFake) Publish(context.Context, messaging.Event) error { return p.err }

type outboxRepositoryFake struct {
	published, retried, failed int
	retryDelay                 time.Duration
	retryCause                 error
}

func (*outboxRepositoryFake) ClaimBatch(context.Context, string, int, time.Duration) ([]model.OutboxEvent, error) {
	return nil, nil
}
func (r *outboxRepositoryFake) MarkPublished(context.Context, string, string) (bool, error) {
	r.published++
	return true, nil
}
func (r *outboxRepositoryFake) ScheduleRetry(_ context.Context, _, _ string, delay time.Duration, cause error) (bool, error) {
	r.retried++
	r.retryDelay, r.retryCause = delay, cause
	return true, nil
}
func (r *outboxRepositoryFake) MarkFailed(context.Context, string, string, error) (bool, error) {
	r.failed++
	return true, nil
}
