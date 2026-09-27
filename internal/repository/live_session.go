package repository

import (
	"context"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"gorm.io/gorm"
)

type LiveSessionRepository interface {
	Create(ctx context.Context, session *model.LiveSession) error
	FindByID(ctx context.Context, id uint64) (*model.LiveSession, error)
	Start(ctx context.Context, id, hostUserID uint64) (bool, error)
	End(ctx context.Context, id, hostUserID uint64, correlationID string) (*model.LiveSession, bool, error)
}

type liveSessionRepository struct {
	db *gorm.DB
}

func NewLiveSessionRepository(db *gorm.DB) LiveSessionRepository {
	return &liveSessionRepository{db: db}
}

func (r *liveSessionRepository) Create(ctx context.Context, session *model.LiveSession) error {
	return r.db.WithContext(ctx).Create(session).Error
}

func (r *liveSessionRepository) FindByID(ctx context.Context, id uint64) (*model.LiveSession, error) {
	var session model.LiveSession
	if err := r.db.WithContext(ctx).First(&session, id).Error; err != nil {
		return nil, err
	}
	return &session, nil
}

func (r *liveSessionRepository) Start(ctx context.Context, id, hostUserID uint64) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&model.LiveSession{}).
		Where("id = ? AND host_user_id = ? AND status = ?", id, hostUserID, model.LiveSessionStatusScheduled).
		Updates(map[string]any{
			"status":     model.LiveSessionStatusLive,
			"started_at": gorm.Expr("UTC_TIMESTAMP(6)"),
			"updated_at": gorm.Expr("UTC_TIMESTAMP(6)"),
		})
	return result.RowsAffected == 1, result.Error
}

func (r *liveSessionRepository) End(ctx context.Context, id, hostUserID uint64, correlationID string) (*model.LiveSession, bool, error) {
	var ended *model.LiveSession
	updated := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var endedAt time.Time
		if err := tx.Raw("SELECT UTC_TIMESTAMP(6)").Scan(&endedAt).Error; err != nil {
			return err
		}
		result := tx.Model(&model.LiveSession{}).
			Where("id = ? AND host_user_id = ? AND status = ?", id, hostUserID, model.LiveSessionStatusLive).
			Updates(map[string]any{"status": model.LiveSessionStatusEnded, "ended_at": endedAt, "updated_at": endedAt})
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		event, err := messaging.NewLiveSessionEndedEvent(id, hostUserID, endedAt.UTC(), correlationID)
		if err != nil {
			return err
		}
		body, err := event.Marshal()
		if err != nil {
			return err
		}
		if err := tx.Create(&model.OutboxEvent{
			EventID:       event.EventID,
			EventType:     string(event.EventType),
			SchemaVersion: uint(event.SchemaVersion),
			Payload:       body,
			Status:        model.OutboxStatusPending,
			AvailableAt:   endedAt,
		}).Error; err != nil {
			return err
		}
		var session model.LiveSession
		if err := tx.First(&session, id).Error; err != nil {
			return err
		}
		ended, updated = &session, true
		return nil
	})
	return ended, updated, err
}
