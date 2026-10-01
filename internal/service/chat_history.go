package service

import (
	"context"
	"errors"

	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/repository"
)

const (
	defaultChatHistoryLimit = 50
	maxChatHistoryLimit     = 100
)

var ErrInvalidChatHistoryLimit = errors.New("limit must be between 1 and 100")

type ChatHistoryPage struct {
	Messages   []model.ChatMessage
	NextCursor uint64
	HasMore    bool
}

type ChatHistoryService interface {
	History(ctx context.Context, liveSessionID, afterID uint64, limit int) (ChatHistoryPage, error)
}

type LiveSessionReader interface {
	Get(context.Context, uint64) (*model.LiveSession, error)
}

type chatHistoryService struct {
	sessions LiveSessionReader
	messages repository.ChatHistoryRepository
}

func NewChatHistoryService(sessions LiveSessionReader, messages repository.ChatHistoryRepository) ChatHistoryService {
	return &chatHistoryService{sessions: sessions, messages: messages}
}

func (s *chatHistoryService) History(ctx context.Context, liveSessionID, afterID uint64, limit int) (ChatHistoryPage, error) {
	if limit == 0 {
		limit = defaultChatHistoryLimit
	}
	if limit < 1 || limit > maxChatHistoryLimit {
		return ChatHistoryPage{}, ErrInvalidChatHistoryLimit
	}
	if _, err := s.sessions.Get(ctx, liveSessionID); err != nil {
		return ChatHistoryPage{}, err
	}
	messages, err := s.messages.ListAfter(ctx, liveSessionID, afterID, limit+1)
	if err != nil {
		return ChatHistoryPage{}, err
	}
	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	nextCursor := afterID
	if len(messages) > 0 {
		nextCursor = messages[len(messages)-1].ID
	}
	return ChatHistoryPage{Messages: messages, NextCursor: nextCursor, HasMore: hasMore}, nil
}
