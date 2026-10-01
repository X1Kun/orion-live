package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/middleware"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/service"
	"github.com/gin-gonic/gin"
)

func TestChatHistoryHandlerReturnsCursorPage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	history := &chatHistoryServiceStub{page: service.ChatHistoryPage{
		Messages:   []model.ChatMessage{{ID: 11, UserID: 42, MessageID: "message", Content: "hello", AcceptedAt: time.Date(2026, 10, 1, 1, 2, 3, 0, time.UTC)}},
		NextCursor: 11,
		HasMore:    true,
	}}
	router := gin.New()
	router.GET("/live-sessions/:id/messages", func(c *gin.Context) {
		c.Set(middleware.UserIDKey, uint64(42))
		NewChatHandler(history).History(c)
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/live-sessions/7/messages?after_id=10&limit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if history.liveSessionID != 7 || history.afterID != 10 || history.limit != 1 {
		t.Fatalf("history query = session:%d after:%d limit:%d", history.liveSessionID, history.afterID, history.limit)
	}
	for _, fragment := range []string{`"id":11`, `"message_id":"message"`, `"next_cursor":11`, `"has_more":true`} {
		if !strings.Contains(response.Body.String(), fragment) {
			t.Fatalf("response missing %s: %s", fragment, response.Body.String())
		}
	}
}

func TestChatHistoryHandlerRejectsInvalidQuery(t *testing.T) {
	tests := []string{
		"/live-sessions/7/messages?after_id=-1",
		"/live-sessions/7/messages?after_id=invalid",
		"/live-sessions/7/messages?limit=0",
		"/live-sessions/7/messages?limit=invalid",
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.GET("/live-sessions/:id/messages", func(c *gin.Context) {
				c.Set(middleware.UserIDKey, uint64(42))
				NewChatHandler(&chatHistoryServiceStub{}).History(c)
			})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestChatHistoryHandlerMapsServiceErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "missing session", err: service.ErrLiveSessionNotFound, status: http.StatusNotFound},
		{name: "invalid limit", err: service.ErrInvalidChatHistoryLimit, status: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.GET("/live-sessions/:id/messages", func(c *gin.Context) {
				c.Set(middleware.UserIDKey, uint64(42))
				NewChatHandler(&chatHistoryServiceStub{err: tt.err}).History(c)
			})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/live-sessions/7/messages", nil))
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
		})
	}
}

type chatHistoryServiceStub struct {
	page          service.ChatHistoryPage
	err           error
	liveSessionID uint64
	afterID       uint64
	limit         int
}

func (s *chatHistoryServiceStub) History(_ context.Context, liveSessionID, afterID uint64, limit int) (service.ChatHistoryPage, error) {
	s.liveSessionID, s.afterID, s.limit = liveSessionID, afterID, limit
	return s.page, s.err
}
