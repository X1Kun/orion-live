package service

import (
	"context"
	"errors"
	"testing"

	"github.com/X1Kun/orion-live/internal/model"
)

func TestChatHistoryUsesStableCursorPagination(t *testing.T) {
	repository := &chatHistoryRepositoryStub{messages: []model.ChatMessage{{ID: 11}, {ID: 12}, {ID: 13}}}
	service := NewChatHistoryService(chatHistorySessionReaderStub{}, repository)
	afterID := uint64(10)
	page, err := service.History(context.Background(), 7, &afterID, 2)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(page.Messages) != 2 || page.NextCursor != 12 || !page.HasMore {
		t.Fatalf("unexpected page: %#v", page)
	}
	if repository.liveSessionID != 7 || repository.afterID != 10 || repository.limit != 3 {
		t.Fatalf("repository query = session:%d after:%d limit:%d", repository.liveSessionID, repository.afterID, repository.limit)
	}
}

func TestChatHistoryDefaultsAndValidatesLimit(t *testing.T) {
	repository := &chatHistoryRepositoryStub{}
	service := NewChatHistoryService(chatHistorySessionReaderStub{}, repository)
	afterID := uint64(9)
	page, err := service.History(context.Background(), 7, &afterID, 0)
	if err != nil {
		t.Fatalf("History() default error = %v", err)
	}
	if page.NextCursor != 9 || page.HasMore || repository.limit != defaultChatHistoryLimit+1 {
		t.Fatalf("unexpected empty page: %#v, repository limit %d", page, repository.limit)
	}
	if _, err := service.History(context.Background(), 7, nil, maxChatHistoryLimit+1); !errors.Is(err, ErrInvalidChatHistoryLimit) {
		t.Fatalf("History() error = %v, want %v", err, ErrInvalidChatHistoryLimit)
	}
}

func TestChatHistoryRequiresExistingSession(t *testing.T) {
	want := ErrLiveSessionNotFound
	service := NewChatHistoryService(chatHistorySessionReaderStub{err: want}, &chatHistoryRepositoryStub{})
	if _, err := service.History(context.Background(), 7, nil, 1); !errors.Is(err, want) {
		t.Fatalf("History() error = %v, want %v", err, want)
	}
}

func TestChatHistoryWithoutCursorReturnsLatestPage(t *testing.T) {
	repository := &chatHistoryRepositoryStub{latest: []model.ChatMessage{{ID: 91}, {ID: 92}}}
	service := NewChatHistoryService(chatHistorySessionReaderStub{}, repository)
	page, err := service.History(context.Background(), 7, nil, 2)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(page.Messages) != 2 || page.NextCursor != 92 || page.HasMore || repository.latestLimit != 2 {
		t.Fatalf("unexpected latest page: %#v, repository limit %d", page, repository.latestLimit)
	}
}

type chatHistorySessionReaderStub struct{ err error }

func (s chatHistorySessionReaderStub) Get(context.Context, uint64) (*model.LiveSession, error) {
	return &model.LiveSession{}, s.err
}

type chatHistoryRepositoryStub struct {
	messages      []model.ChatMessage
	latest        []model.ChatMessage
	err           error
	liveSessionID uint64
	afterID       uint64
	limit         int
	latestLimit   int
}

func (r *chatHistoryRepositoryStub) ListLatest(_ context.Context, liveSessionID uint64, limit int) ([]model.ChatMessage, error) {
	r.liveSessionID, r.latestLimit = liveSessionID, limit
	return r.latest, r.err
}

func (r *chatHistoryRepositoryStub) ListAfter(_ context.Context, liveSessionID, afterID uint64, limit int) ([]model.ChatMessage, error) {
	r.liveSessionID, r.afterID, r.limit = liveSessionID, afterID, limit
	return r.messages, r.err
}
