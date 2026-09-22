package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
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
	publisher := &Publisher{client: &Client{closed: true}}
	err := publisher.Publish(context.Background(), publisherTestEvent())
	if !errors.Is(err, ErrPublishInterrupted) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrPublishInterrupted)
	}
	if errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("Publish() error = %v unexpectedly matched %v", err, ErrPublisherClosed)
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
