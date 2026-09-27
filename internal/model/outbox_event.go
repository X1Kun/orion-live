package model

import "time"

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusClaimed   OutboxStatus = "CLAIMED"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

type OutboxEvent struct {
	EventID       string       `gorm:"primaryKey;size:64"`
	EventType     string       `gorm:"size:64;not null"`
	SchemaVersion uint         `gorm:"not null"`
	Payload       []byte       `gorm:"type:json;not null"`
	Status        OutboxStatus `gorm:"type:varchar(16);not null"`
	AvailableAt   time.Time    `gorm:"not null"`
	ClaimedBy     *string      `gorm:"size:128"`
	ClaimToken    *string      `gorm:"size:32"`
	LeaseUntil    *time.Time
	AttemptCount  uint
	LastError     *string `gorm:"size:1024"`
	PublishedAt   *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
