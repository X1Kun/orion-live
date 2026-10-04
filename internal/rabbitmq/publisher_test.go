package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
)

func TestPublishDistinguishesClosedPublisher(t *testing.T) {
	publisher := &Publisher{closed: true}
	err := publisher.Publish(context.Background(), publisherTestEvent())
	if !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrPublisherClosed)
	}
}

func TestPublishClassifiesClosedClientAsInterrupted(t *testing.T) {
	lane := &publisherLane{client: &Client{closed: true}}
	available := make(chan publishLane, 1)
	available <- lane
	publisher := &Publisher{lanes: []publishLane{lane}, available: available}
	err := publisher.Publish(context.Background(), publisherTestEvent())
	if !errors.Is(err, ErrPublishInterrupted) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrPublishInterrupted)
	}
	if errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("Publish() error = %v unexpectedly matched %v", err, ErrPublisherClosed)
	}
}

func TestNewPublisherRejectsInvalidConcurrency(t *testing.T) {
	for _, concurrency := range []int{0, maxPublisherConcurrency + 1} {
		if _, err := NewPublisher(context.Background(), nil, concurrency); !errors.Is(err, ErrInvalidPublisherConcurrency) {
			t.Errorf("NewPublisher(concurrency=%d) error = %v, want %v", concurrency, err, ErrInvalidPublisherConcurrency)
		}
	}
}

func TestPublisherAcquireHonorsBoundAndContext(t *testing.T) {
	lane := &publisherLane{}
	available := make(chan publishLane, 1)
	available <- lane
	publisher := &Publisher{lanes: []publishLane{lane}, available: available}

	borrowed, err := publisher.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := publisher.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire error = %v, want %v", err, context.DeadlineExceeded)
	}
	publisher.release(borrowed)
}

func TestPublisherUsesIndependentLanesConcurrently(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	first := &blockingPublishLane{entered: entered, release: release}
	second := &blockingPublishLane{entered: entered, release: release}
	available := make(chan publishLane, 2)
	available <- first
	available <- second
	publisher := &Publisher{lanes: []publishLane{first, second}, available: available}

	errorsCh := make(chan error, 2)
	var publishers sync.WaitGroup
	publishers.Add(2)
	for range 2 {
		go func() {
			defer publishers.Done()
			errorsCh <- publisher.Publish(context.Background(), publisherTestEvent())
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("concurrent publication did not enter a second lane")
		}
	}
	close(release)
	publishers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}
}

func TestPublishResultLabelsAreBounded(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: nil, want: "confirmed"},
		{err: context.DeadlineExceeded, want: "timeout"},
		{err: context.Canceled, want: "canceled"},
		{err: ErrPublisherClosed, want: "closed"},
		{err: ErrPublishUnroutable, want: "unroutable"},
		{err: ErrPublishNacked, want: "nacked"},
		{err: ErrPublishInterrupted, want: "interrupted"},
		{err: messaging.ErrInvalidEvent, want: "invalid"},
		{err: errors.New("unexpected"), want: "error"},
	}
	for _, tt := range tests {
		if got := publishResult(tt.err); got != tt.want {
			t.Errorf("publishResult(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func publisherTestEvent() messaging.Event {
	return messaging.Event{
		EventID:       "publisher-test-event",
		EventType:     messaging.EventTypeChatMessageAccepted,
		SchemaVersion: messaging.SchemaVersionV1,
		CorrelationID: "publisher-test-correlation",
		UserID:        1,
		LiveSessionID: 1,
		OccurredAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`{"message_id":"message-1","content":"hello"}`),
	}
}

type blockingPublishLane struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (l *blockingPublishLane) publish(ctx context.Context, _ messaging.Event, _ []byte) error {
	l.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.release:
		return nil
	}
}

func (*blockingPublishLane) close() error { return nil }
