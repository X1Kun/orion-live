package messaging

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidChatPayload = errors.New("invalid Chat message payload")

type ChatMessageAcceptedPayload struct {
	MessageID  string    `json:"message_id"`
	Content    string    `json:"content"`
	AcceptedAt time.Time `json:"accepted_at"`
}

func (p ChatMessageAcceptedPayload) Validate() error {
	if len(p.MessageID) != 36 || p.MessageID[8] != '-' || p.MessageID[13] != '-' || p.MessageID[18] != '-' || p.MessageID[23] != '-' || p.MessageID[14] != '4' {
		return ErrInvalidChatPayload
	}
	variant := p.MessageID[19]
	if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
		return ErrInvalidChatPayload
	}
	for i := range len(p.MessageID) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if value := p.MessageID[i]; !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return ErrInvalidChatPayload
		}
	}
	if !utf8.ValidString(p.Content) || strings.TrimSpace(p.Content) == "" || utf8.RuneCountInString(p.Content) > 500 {
		return ErrInvalidChatPayload
	}
	if p.AcceptedAt.IsZero() {
		return ErrInvalidChatPayload
	}
	_, offset := p.AcceptedAt.Zone()
	if offset != 0 {
		return ErrInvalidChatPayload
	}
	return nil
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
	chatPayload := ChatMessageAcceptedPayload{
		MessageID:  messageID,
		Content:    content,
		AcceptedAt: acceptedAt,
	}
	if err := chatPayload.Validate(); err != nil {
		return Event{}, err
	}
	payload, err := json.Marshal(chatPayload)
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
