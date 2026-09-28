package messaging

import (
	"encoding/json"
	"time"
)

type ChatMessageAcceptedPayload struct {
	MessageID  string    `json:"message_id"`
	Content    string    `json:"content"`
	AcceptedAt time.Time `json:"accepted_at"`
}

func NewChatMessageAcceptedEvent(
	liveSessionID, userID uint64,
	messageID, content string,
	acceptedAt time.Time,
) (Event, error) {
	eventID, err := NewEventID()
	if err != nil {
		return Event{}, err
	}
	correlationID, err := NewCorrelationID()
	if err != nil {
		return Event{}, err
	}
	payload, err := json.Marshal(ChatMessageAcceptedPayload{
		MessageID:  messageID,
		Content:    content,
		AcceptedAt: acceptedAt,
	})
	if err != nil {
		return Event{}, err
	}
	event := Event{
		EventID:       eventID,
		EventType:     EventTypeChatMessageAccepted,
		SchemaVersion: SchemaVersionV1,
		CorrelationID: correlationID,
		UserID:        userID,
		LiveSessionID: liveSessionID,
		OccurredAt:    acceptedAt,
		Payload:       payload,
	}
	return event, event.Validate()
}
