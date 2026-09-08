package handler

import (
	"bytes"
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

func TestLiveSessionHandlerCreate(t *testing.T) {
	var gotHostUserID uint64
	service := &liveSessionServiceStub{
		create: func(_ context.Context, hostUserID uint64, title, coverURL string) (*model.LiveSession, error) {
			gotHostUserID = hostUserID
			createdAt := time.Date(2026, 9, 7, 14, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
			return &model.LiveSession{
				BaseModel:  model.BaseModel{ID: 9, CreatedAt: createdAt, UpdatedAt: createdAt},
				HostUserID: hostUserID,
				Title:      title,
				Status:     model.LiveSessionStatusScheduled,
			}, nil
		},
	}
	handler := NewLiveSessionHandler(service)
	router := gin.New()
	router.POST("/live-sessions", func(c *gin.Context) {
		c.Set(middleware.UserIDKey, uint64(42))
		handler.Create(c)
	})

	request := httptest.NewRequest(http.MethodPost, "/live-sessions", bytes.NewBufferString(`{"title":"Launch Stream"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusCreated, response.Body.String())
	}
	if gotHostUserID != 42 {
		t.Fatalf("host user ID = %d, want 42", gotHostUserID)
	}
	if !strings.Contains(response.Body.String(), `"created_at":"2026-09-07T06:00:00Z"`) {
		t.Fatalf("created_at was not normalized to UTC; body = %s", response.Body.String())
	}
}

func TestLiveSessionHandlerMapsDomainErrors(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		register   func(*gin.Engine, *LiveSessionHandler)
		service    *liveSessionServiceStub
		wantStatus int
	}{
		{
			name:   "not found",
			method: http.MethodGet,
			path:   "/live-sessions/8",
			register: func(router *gin.Engine, handler *LiveSessionHandler) {
				router.GET("/live-sessions/:id", handler.Get)
			},
			service: &liveSessionServiceStub{
				get: func(context.Context, uint64) (*model.LiveSession, error) {
					return nil, service.ErrLiveSessionNotFound
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "invalid id",
			method: http.MethodGet,
			path:   "/live-sessions/not-a-number",
			register: func(router *gin.Engine, handler *LiveSessionHandler) {
				router.GET("/live-sessions/:id", handler.Get)
			},
			service:    &liveSessionServiceStub{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "host already live",
			method: http.MethodPost,
			path:   "/live-sessions/8/start",
			register: func(router *gin.Engine, handler *LiveSessionHandler) {
				router.POST("/live-sessions/:id/start", func(c *gin.Context) {
					c.Set(middleware.UserIDKey, uint64(42))
					handler.Start(c)
				})
			},
			service: &liveSessionServiceStub{
				start: func(context.Context, uint64, uint64) (*model.LiveSession, error) {
					return nil, service.ErrHostAlreadyLive
				},
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewLiveSessionHandler(tt.service)
			router := gin.New()
			tt.register(router, handler)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(tt.method, tt.path, nil))
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
		})
	}
}

type liveSessionServiceStub struct {
	create func(context.Context, uint64, string, string) (*model.LiveSession, error)
	get    func(context.Context, uint64) (*model.LiveSession, error)
	start  func(context.Context, uint64, uint64) (*model.LiveSession, error)
	end    func(context.Context, uint64, uint64) (*model.LiveSession, error)
}

func (s *liveSessionServiceStub) Create(ctx context.Context, hostUserID uint64, title, coverURL string) (*model.LiveSession, error) {
	return s.create(ctx, hostUserID, title, coverURL)
}

func (s *liveSessionServiceStub) Get(ctx context.Context, id uint64) (*model.LiveSession, error) {
	if s.get == nil {
		return nil, nil
	}
	return s.get(ctx, id)
}

func (s *liveSessionServiceStub) Start(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error) {
	return s.start(ctx, id, hostUserID)
}

func (s *liveSessionServiceStub) End(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error) {
	return s.end(ctx, id, hostUserID)
}
