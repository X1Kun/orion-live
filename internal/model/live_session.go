package model

import "time"

type LiveSessionStatus string

const (
	LiveSessionStatusScheduled LiveSessionStatus = "SCHEDULED"
	LiveSessionStatusLive      LiveSessionStatus = "LIVE"
	LiveSessionStatusEnded     LiveSessionStatus = "ENDED"
)

type LiveSession struct {
	BaseModel
	HostUserID uint64            `gorm:"not null;index:idx_live_sessions_host_status_id,priority:1"`
	Title      string            `gorm:"size:255;not null"`
	CoverURL   *string           `gorm:"size:2048"`
	Status     LiveSessionStatus `gorm:"type:varchar(16);not null;index:idx_live_sessions_host_status_id,priority:2"`
	StartedAt  *time.Time
	EndedAt    *time.Time
}
