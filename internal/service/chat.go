package service

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
)

const maxChatContentRunes = 500

var (
	ErrInvalidChatMessageID = errors.New("message_id must be a UUIDv4")
	ErrInvalidChatContent   = errors.New("content must contain 1 to 500 characters")
)

type ChatAcceptance struct {
	MessageID  string
	AcceptedAt time.Time
}

type ChatService interface {
	Accept(ctx context.Context, liveSessionID, userID uint64, messageID, content string) (ChatAcceptance, error)
}

type ChatEventPublisher interface {
	Publish(context.Context, messaging.Event) error
}

type chatService struct {
	publisher ChatEventPublisher
	config    config.Chat
}

func NewChatService(publisher ChatEventPublisher, cfg config.Chat) ChatService {
	return &chatService{publisher: publisher, config: cfg}
}

func (s *chatService) Accept(ctx context.Context, liveSessionID, userID uint64, messageID, content string) (ChatAcceptance, error) {
	messageID = strings.ToLower(messageID)
	if !isUUIDv4(messageID) {
		return ChatAcceptance{}, ErrInvalidChatMessageID
	}
	if !utf8.ValidString(content) || strings.TrimSpace(content) == "" || utf8.RuneCountInString(content) > maxChatContentRunes {
		return ChatAcceptance{}, ErrInvalidChatContent
	}

	acceptedAt := time.Now().UTC()
	event, err := messaging.NewChatMessageAcceptedEvent(liveSessionID, userID, messageID, content, acceptedAt)
	if err != nil {
		return ChatAcceptance{}, err
	}
	publishCtx, cancel := context.WithTimeout(ctx, s.config.PublishTimeout)
	defer cancel()
	if err := s.publisher.Publish(publishCtx, event); err != nil {
		return ChatAcceptance{}, err
	}
	return ChatAcceptance{MessageID: messageID, AcceptedAt: acceptedAt}, nil
}

func isUUIDv4(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value[14] != '4' {
		return false
	}
	if value[19] != '8' && value[19] != '9' && value[19] != 'a' && value[19] != 'b' {
		return false
	}
	for i := range len(value) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !isLowerHex(value[i]) {
			return false
		}
	}
	return true
}

func isLowerHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f'
}
