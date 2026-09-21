package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	SchemaVersionV1 = 1

	ExchangeName = "orion.interaction.events"

	EventTypeLiveSessionEnded    EventType = "live_session.ended"
	EventTypeChatMessageAccepted EventType = "chat.message.accepted"
)

var (
	ErrInvalidEvent = errors.New("invalid interaction event")
)

type EventType string

type Event struct {
	EventID       string          `json:"event_id"`
	EventType     EventType       `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	CorrelationID string          `json:"correlation_id"`
	UserID        uint64          `json:"user_id,omitempty"`
	LiveSessionID uint64          `json:"live_session_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

func (e Event) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}
	if e.EventType != EventTypeLiveSessionEnded && e.EventType != EventTypeChatMessageAccepted {
		return fmt.Errorf("%w: unsupported event_type %q", ErrInvalidEvent, e.EventType)
	}
	if e.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("%w: unsupported schema_version %d", ErrInvalidEvent, e.SchemaVersion)
	}
	if e.CorrelationID == "" {
		return fmt.Errorf("%w: correlation_id is required", ErrInvalidEvent)
	}
	if e.LiveSessionID == 0 {
		return fmt.Errorf("%w: live_session_id must be positive", ErrInvalidEvent)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidEvent)
	}
	_, offset := e.OccurredAt.Zone()
	if offset != 0 {
		return fmt.Errorf("%w: occurred_at must use UTC", ErrInvalidEvent)
	}
	if len(e.Payload) == 0 || !json.Valid(e.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidEvent)
	}
	return nil
}

func (e Event) RoutingKey() string {
	return string(e.EventType)
}

func (e Event) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}
