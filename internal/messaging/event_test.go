package messaging

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEventMarshal(t *testing.T) {
	event := validEvent()
	body, err := event.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded Event
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if decoded.EventID != event.EventID || decoded.RoutingKey() != string(EventTypeChatMessageAccepted) {
		t.Fatalf("decoded event = %#v", decoded)
	}
}

func TestEventValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
	}{
		{name: "missing event id", mutate: func(e *Event) { e.EventID = "" }},
		{name: "unknown type", mutate: func(e *Event) { e.EventType = "unknown" }},
		{name: "wrong schema", mutate: func(e *Event) { e.SchemaVersion = 2 }},
		{name: "missing correlation", mutate: func(e *Event) { e.CorrelationID = "" }},
		{name: "missing session", mutate: func(e *Event) { e.LiveSessionID = 0 }},
		{name: "non UTC time", mutate: func(e *Event) { e.OccurredAt = e.OccurredAt.In(time.FixedZone("UTC+8", 8*60*60)) }},
		{name: "invalid payload", mutate: func(e *Event) { e.Payload = json.RawMessage("{") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := validEvent()
			tt.mutate(&event)
			if err := event.Validate(); !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrInvalidEvent)
			}
		})
	}
}

func TestEventTypeIsSupported(t *testing.T) {
	if !EventTypeLiveSessionEnded.IsSupported() || !EventTypeChatMessageAccepted.IsSupported() {
		t.Fatal("core event type was not supported")
	}
	if EventType("unknown").IsSupported() {
		t.Fatal("unknown event type was supported")
	}
}

func validEvent() Event {
	return Event{
		EventID:       "event-1",
		EventType:     EventTypeChatMessageAccepted,
		SchemaVersion: SchemaVersionV1,
		CorrelationID: "correlation-1",
		UserID:        1,
		LiveSessionID: 1,
		OccurredAt:    time.Date(2026, 9, 20, 1, 2, 3, 0, time.UTC),
		Payload:       json.RawMessage(`{"message_id":"message-1","content":"hello"}`),
	}
}
