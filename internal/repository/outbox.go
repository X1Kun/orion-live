package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"gorm.io/gorm"
)

type OutboxRepository interface {
	ClaimBatch(ctx context.Context, owner string, limit int, lease time.Duration) ([]model.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID, claimToken string) (bool, error)
	ScheduleRetry(ctx context.Context, eventID, claimToken string, delay time.Duration, cause error) (bool, error)
	MarkFailed(ctx context.Context, eventID, claimToken string, cause error) (bool, error)
}

type outboxRepository struct{ db *gorm.DB }

func NewOutboxRepository(db *gorm.DB) OutboxRepository { return &outboxRepository{db: db} }

func (r *outboxRepository) ClaimBatch(ctx context.Context, owner string, limit int, lease time.Duration) ([]model.OutboxEvent, error) {
	var claimed []model.OutboxEvent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var candidates []model.OutboxEvent
		if err := tx.Raw(`SELECT * FROM outbox_events
WHERE (status = 'PENDING' AND available_at <= UTC_TIMESTAMP(6))
   OR (status = 'CLAIMED' AND lease_until <= UTC_TIMESTAMP(6))
ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED`, limit).Scan(&candidates).Error; err != nil {
			return err
		}
		for i := range candidates {
			token, err := messaging.NewClaimToken()
			if err != nil {
				return err
			}
			result := tx.Model(&model.OutboxEvent{}).
				Where("event_id = ?", candidates[i].EventID).
				Updates(map[string]any{
					"status":        model.OutboxStatusClaimed,
					"claimed_by":    owner,
					"claim_token":   token,
					"lease_until":   gorm.Expr("DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)", lease.Microseconds()),
					"attempt_count": gorm.Expr("attempt_count + 1"),
					"updated_at":    gorm.Expr("UTC_TIMESTAMP(6)"),
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("claim Outbox event %q: expected one updated row, got %d", candidates[i].EventID, result.RowsAffected)
			}
			candidates[i].Status = model.OutboxStatusClaimed
			candidates[i].ClaimedBy, candidates[i].ClaimToken = &owner, &token
			candidates[i].AttemptCount++
			claimed = append(claimed, candidates[i])
		}
		return nil
	})
	return claimed, err
}

func (r *outboxRepository) MarkPublished(ctx context.Context, eventID, claimToken string) (bool, error) {
	result := r.db.WithContext(ctx).Model(&model.OutboxEvent{}).
		Where("event_id = ? AND status = ? AND claim_token = ?", eventID, model.OutboxStatusClaimed, claimToken).
		Updates(map[string]any{
			"status": model.OutboxStatusPublished, "published_at": gorm.Expr("UTC_TIMESTAMP(6)"),
			"claimed_by": nil, "claim_token": nil, "lease_until": nil, "updated_at": gorm.Expr("UTC_TIMESTAMP(6)"),
		})
	return result.RowsAffected == 1, result.Error
}

func (r *outboxRepository) ScheduleRetry(ctx context.Context, eventID, claimToken string, delay time.Duration, cause error) (bool, error) {
	return r.updateFailure(ctx, eventID, claimToken, model.OutboxStatusPending, delay, cause)
}

func (r *outboxRepository) MarkFailed(ctx context.Context, eventID, claimToken string, cause error) (bool, error) {
	return r.updateFailure(ctx, eventID, claimToken, model.OutboxStatusFailed, 0, cause)
}

func (r *outboxRepository) updateFailure(ctx context.Context, eventID, claimToken string, status model.OutboxStatus, delay time.Duration, cause error) (bool, error) {
	message := truncateError(cause)
	updates := map[string]any{
		"status": status, "last_error": message, "claimed_by": nil, "claim_token": nil,
		"lease_until": nil, "updated_at": gorm.Expr("UTC_TIMESTAMP(6)"),
	}
	if status == model.OutboxStatusPending {
		updates["available_at"] = gorm.Expr("DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)", delay.Microseconds())
	}
	result := r.db.WithContext(ctx).Model(&model.OutboxEvent{}).
		Where("event_id = ? AND status = ? AND claim_token = ?", eventID, model.OutboxStatusClaimed, claimToken).
		Updates(updates)
	return result.RowsAffected == 1, result.Error
}

func truncateError(err error) string {
	if err == nil {
		return "unknown error"
	}
	message := strings.TrimSpace(err.Error())
	runes := []rune(message)
	if len(runes) > 1024 {
		return string(runes[:1024])
	}
	return message
}
