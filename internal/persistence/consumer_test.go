package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/repository"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHandleDeliveryClassifiesOutcomes(t *testing.T) {
	databaseErr := errors.New("database unavailable")
	tests := []struct {
		name          string
		body          func(*testing.T) []byte
		result        repository.ChatPersistenceResult
		repositoryErr error
		wantAck       int
		wantReject    int
		wantRequeue   bool
	}{
		{name: "persisted", body: persistenceTestBody, result: repository.ChatPersisted, wantAck: 1},
		{name: "duplicate event", body: persistenceTestBody, result: repository.ChatDuplicateEvent, wantAck: 1},
		{name: "duplicate message", body: persistenceTestBody, result: repository.ChatDuplicateMessage, wantAck: 1},
		{name: "conflict", body: persistenceTestBody, result: repository.ChatConflictingMessage, wantReject: 1},
		{name: "retryable", body: persistenceTestBody, repositoryErr: databaseErr, wantReject: 1, wantRequeue: true},
		{name: "malformed", body: func(*testing.T) []byte { return []byte("{") }, wantReject: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acknowledger := &persistenceAcknowledger{}
			repositoryFake := &chatRepositoryFake{result: tt.result, err: tt.repositoryErr}
			consumer := &Consumer{
				repository: repositoryFake,
				config:     config.Persistence{ProcessingTimeout: time.Second},
				ctx:        context.Background(),
			}
			delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1, Headers: amqp.Table{}, Body: tt.body(t)}
			if err := consumer.handleDelivery(delivery); err != nil {
				t.Fatalf("handleDelivery() error = %v", err)
			}
			if acknowledger.acks != tt.wantAck || acknowledger.rejects != tt.wantReject || acknowledger.requeue != tt.wantRequeue {
				t.Fatalf("acks=%d rejects=%d requeue=%v", acknowledger.acks, acknowledger.rejects, acknowledger.requeue)
			}
		})
	}
}

func persistenceTestBody(t *testing.T) []byte {
	t.Helper()
	event, err := messaging.NewChatMessageAcceptedEvent(
		7, 42, "018f47a2-8e31-4f10-8af0-2bdac5812501", "hello", time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	body, err := event.Marshal()
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return body
}

type chatRepositoryFake struct {
	result repository.ChatPersistenceResult
	err    error
}

func (r *chatRepositoryFake) Persist(context.Context, string, messaging.Event, messaging.ChatMessageAcceptedPayload) (repository.ChatPersistenceResult, error) {
	return r.result, r.err
}

type persistenceAcknowledger struct {
	acks    int
	rejects int
	requeue bool
}

func (a *persistenceAcknowledger) Ack(uint64, bool) error {
	a.acks++
	return nil
}

func (a *persistenceAcknowledger) Nack(uint64, bool, bool) error {
	return nil
}

func (a *persistenceAcknowledger) Reject(_ uint64, requeue bool) error {
	a.rejects++
	a.requeue = requeue
	return nil
}
