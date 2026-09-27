package messaging

import (
	"testing"
	"time"
)

func TestLiveSessionEndedEventIsStable(t *testing.T) {
	endedAt := time.Date(2026, 9, 23, 1, 2, 3, 0, time.UTC)
	first, err := NewLiveSessionEndedEvent(7, 42, endedAt, "correlation-a")
	if err != nil {
		t.Fatalf("first event: %v", err)
	}
	second, err := NewLiveSessionEndedEvent(7, 42, endedAt, "correlation-b")
	if err != nil {
		t.Fatalf("second event: %v", err)
	}
	if first.EventID != second.EventID {
		t.Fatalf("event IDs differ: %q != %q", first.EventID, second.EventID)
	}
	if len(first.EventID) != 64 {
		t.Fatalf("event ID length = %d", len(first.EventID))
	}
}
