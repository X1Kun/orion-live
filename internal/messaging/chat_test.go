package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewChatMessageAcceptedEvent(t *testing.T) {
	acceptedAt := time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC)
	event, err := NewChatMessageAcceptedEvent(7, 42, "018f47a2-8e31-4f10-8af0-2bdac5812501", "hello", acceptedAt)
	if err != nil {
		t.Fatalf("NewChatMessageAcceptedEvent() error = %v", err)
	}
	if len(event.EventID) != 32 || len(event.CorrelationID) != 32 {
		t.Fatalf("unexpected generated IDs: event=%q correlation=%q", event.EventID, event.CorrelationID)
	}
	if event.UserID != 42 || event.LiveSessionID != 7 || event.OccurredAt != acceptedAt {
		t.Fatalf("unexpected envelope: %#v", event)
	}
	var payload ChatMessageAcceptedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.MessageID != "018f47a2-8e31-4f10-8af0-2bdac5812501" || payload.Content != "hello" || payload.AcceptedAt != acceptedAt {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}
