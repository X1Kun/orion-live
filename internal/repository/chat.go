package repository

import (
	"context"
	"errors"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errConflictingChatMessage = errors.New("conflicting Chat message")

type ChatPersistenceResult string

const (
	ChatPersisted          ChatPersistenceResult = "persisted"
	ChatDuplicateEvent     ChatPersistenceResult = "duplicate_event"
	ChatDuplicateMessage   ChatPersistenceResult = "duplicate_message"
	ChatConflictingMessage ChatPersistenceResult = "conflicting_message"
)

type ChatPersistenceRepository interface {
	Persist(ctx context.Context, consumerName string, event messaging.Event, payload messaging.ChatMessageAcceptedPayload) (ChatPersistenceResult, error)
}

type ChatHistoryRepository interface {
	ListAfter(ctx context.Context, liveSessionID, afterID uint64, limit int) ([]model.ChatMessage, error)
	ListLatest(ctx context.Context, liveSessionID uint64, limit int) ([]model.ChatMessage, error)
}

func (r *chatRepository) ListLatest(ctx context.Context, liveSessionID uint64, limit int) ([]model.ChatMessage, error) {
	var messages []model.ChatMessage
	err := r.db.WithContext(ctx).
		Where("live_session_id = ?", liveSessionID).
		Order("id DESC").
		Limit(limit).
		Find(&messages).Error
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
	return messages, err
}

type chatRepository struct{ db *gorm.DB }

func NewChatRepository(db *gorm.DB) *chatRepository {
	return &chatRepository{db: db}
}

func (r *chatRepository) ListAfter(ctx context.Context, liveSessionID, afterID uint64, limit int) ([]model.ChatMessage, error) {
	var messages []model.ChatMessage
	err := r.db.WithContext(ctx).
		Where("live_session_id = ? AND id > ?", liveSessionID, afterID).
		Order("id ASC").
		Limit(limit).
		Find(&messages).Error
	return messages, err
}

func (r *chatRepository) Persist(
	ctx context.Context,
	consumerName string,
	event messaging.Event,
	payload messaging.ChatMessageAcceptedPayload,
) (ChatPersistenceResult, error) {
	result := ChatPersisted
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		inbox := model.ConsumerInbox{
			ConsumerName: consumerName,
			EventID:      event.EventID,
			EventType:    string(event.EventType),
		}
		insertInbox := tx.Omit("ProcessedAt").Clauses(clause.OnConflict{DoNothing: true}).Create(&inbox)
		if insertInbox.Error != nil {
			return insertInbox.Error
		}
		if insertInbox.RowsAffected == 0 {
			result = ChatDuplicateEvent
			return nil
		}

		message := model.ChatMessage{
			EventID:       event.EventID,
			LiveSessionID: event.LiveSessionID,
			UserID:        event.UserID,
			MessageID:     payload.MessageID,
			Content:       payload.Content,
			AcceptedAt:    payload.AcceptedAt,
		}
		insertMessage := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&message)
		if insertMessage.Error != nil {
			return insertMessage.Error
		}
		if insertMessage.RowsAffected == 1 {
			return nil
		}

		var existing model.ChatMessage
		if err := tx.Where(
			"live_session_id = ? AND user_id = ? AND message_id = ?",
			event.LiveSessionID,
			event.UserID,
			payload.MessageID,
		).First(&existing).Error; err != nil {
			return err
		}
		if existing.Content == payload.Content {
			result = ChatDuplicateMessage
		} else {
			result = ChatConflictingMessage
			return errConflictingChatMessage
		}
		return nil
	})
	if errors.Is(err, errConflictingChatMessage) {
		return result, nil
	}
	return result, err
}
