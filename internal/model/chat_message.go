package model

import "time"

type ChatMessage struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	EventID       string `gorm:"size:64;not null"`
	LiveSessionID uint64 `gorm:"not null;uniqueIndex:uk_chat_business_key"`
	UserID        uint64 `gorm:"not null;uniqueIndex:uk_chat_business_key"`
	MessageID     string `gorm:"size:36;not null;uniqueIndex:uk_chat_business_key"`
	Content       string `gorm:"size:500;not null"`
	AcceptedAt    time.Time
	CreatedAt     time.Time
}

type ConsumerInbox struct {
	ConsumerName string `gorm:"primaryKey;size:64"`
	EventID      string `gorm:"primaryKey;size:64"`
	EventType    string `gorm:"size:64;not null"`
	ProcessedAt  time.Time
}

func (ConsumerInbox) TableName() string {
	return "consumer_inbox"
}
