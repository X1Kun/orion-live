package repository

import (
	"context"

	"github.com/X1Kun/orion-live/internal/model"
	"gorm.io/gorm"
)

type LiveSessionRepository interface {
	Create(ctx context.Context, session *model.LiveSession) error
	FindByID(ctx context.Context, id uint64) (*model.LiveSession, error)
	Start(ctx context.Context, id, hostUserID uint64) (bool, error)
	End(ctx context.Context, id, hostUserID uint64) (bool, error)
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

func (r *liveSessionRepository) End(ctx context.Context, id, hostUserID uint64) (bool, error) {
	result := r.db.WithContext(ctx).
		Model(&model.LiveSession{}).
		Where("id = ? AND host_user_id = ? AND status = ?", id, hostUserID, model.LiveSessionStatusLive).
		Updates(map[string]any{
			"status":     model.LiveSessionStatusEnded,
			"ended_at":   gorm.Expr("UTC_TIMESTAMP(6)"),
			"updated_at": gorm.Expr("UTC_TIMESTAMP(6)"),
		})
	return result.RowsAffected == 1, result.Error
}
