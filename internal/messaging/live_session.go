package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type LiveSessionEndedPayload struct {
	LiveSessionID uint64    `json:"live_session_id"`
	HostUserID    uint64    `json:"host_user_id"`
	EndedAt       time.Time `json:"ended_at"`
}

func NewLiveSessionEndedEvent(liveSessionID, hostUserID uint64, endedAt time.Time, correlationID string) (Event, error) {
	payload, err := json.Marshal(LiveSessionEndedPayload{LiveSessionID: liveSessionID, HostUserID: hostUserID, EndedAt: endedAt})
	if err != nil {
		return Event{}, err
	}
	event := Event{
		EventID:       stableEventID(EventTypeLiveSessionEnded, liveSessionID),
		EventType:     EventTypeLiveSessionEnded,
		SchemaVersion: SchemaVersionV1,
		CorrelationID: correlationID,
		UserID:        hostUserID,
		LiveSessionID: liveSessionID,
		OccurredAt:    endedAt,
		Payload:       payload,
	}
	return event, event.Validate()
}

func stableEventID(eventType EventType, liveSessionID uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", eventType, liveSessionID)))
	return hex.EncodeToString(digest[:])
}
