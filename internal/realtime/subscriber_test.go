package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHandleBroadcastsAndAcknowledgesEvent(t *testing.T) {
	hub := newRealtimeTestHub(t)
	client, err := roomhub.NewClient(1)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if err := hub.Join(1, client); err != nil {
		t.Fatalf("Hub.Join() error = %v", err)
	}
	acknowledger := &realtimeAcknowledger{}
	delivery := realtimeDelivery(t, acknowledger, validRealtimeEvent())

	subscriber := &Subscriber{hub: hub}
	if err := subscriber.handleDelivery(delivery); err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if acknowledger.acked != 1 {
		t.Fatalf("Ack calls = %d, want 1", acknowledger.acked)
	}
	select {
	case body := <-client.Outbound():
		var event messaging.Event
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatalf("unmarshal outbound event: %v", err)
		}
		if event.EventID != "realtime-event" {
			t.Fatalf("event ID = %q", event.EventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for realtime event")
	}
}

func TestHandleAcknowledgesDroppedEvents(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "no room", body: marshalRealtimeEvent(t, validRealtimeEvent())},
		{name: "malformed", body: []byte("{")},
		{name: "unsupported type", body: []byte(`{"event_id":"event","event_type":"unknown"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hub := newRealtimeTestHub(t)
			acknowledger := &realtimeAcknowledger{}
			delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1, Body: tt.body}
			if err := (&Subscriber{hub: hub}).handleDelivery(delivery); err != nil {
				t.Fatalf("handle() error = %v", err)
			}
			if acknowledger.acked != 1 {
				t.Fatalf("Ack calls = %d, want 1", acknowledger.acked)
			}
			if hub.RoomCount() != 0 {
				t.Fatal("dropped event created a room")
			}
		})
	}
}

func TestHandleReturnsAcknowledgementError(t *testing.T) {
	want := errors.New("ack failed")
	acknowledger := &realtimeAcknowledger{ackErr: want}
	delivery := realtimeDelivery(t, acknowledger, validRealtimeEvent())
	err := (&Subscriber{hub: newRealtimeTestHub(t)}).handleDelivery(delivery)
	if !errors.Is(err, want) {
		t.Fatalf("handle() error = %v, want %v", err, want)
	}
}

func newRealtimeTestHub(t *testing.T) *roomhub.Hub {
	t.Helper()
	hub, err := roomhub.NewHub(4)
	if err != nil {
		t.Fatalf("NewHub() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := hub.Shutdown(ctx); err != nil {
			t.Errorf("Hub.Shutdown() error = %v", err)
		}
	})
	return hub
}

func realtimeDelivery(t *testing.T, acknowledger amqp.Acknowledger, event messaging.Event) amqp.Delivery {
	t.Helper()
	return amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         marshalRealtimeEvent(t, event),
	}
}

func marshalRealtimeEvent(t *testing.T, event messaging.Event) []byte {
	t.Helper()
	body, err := event.Marshal()
	if err != nil {
		t.Fatalf("Event.Marshal() error = %v", err)
	}
	return body
}

func validRealtimeEvent() messaging.Event {
	return messaging.Event{
		EventID:       "realtime-event",
		EventType:     messaging.EventTypeChatMessageAccepted,
		SchemaVersion: messaging.SchemaVersionV1,
		CorrelationID: "correlation",
		UserID:        1,
		LiveSessionID: 1,
		OccurredAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`{"message_id":"message-1","content":"hello"}`),
	}
}

type realtimeAcknowledger struct {
	acked  int
	ackErr error
}

func (a *realtimeAcknowledger) Ack(uint64, bool) error {
	a.acked++
	return a.ackErr
}

func (*realtimeAcknowledger) Nack(uint64, bool, bool) error {
	return nil
}

func (*realtimeAcknowledger) Reject(uint64, bool) error {
	return nil
}
