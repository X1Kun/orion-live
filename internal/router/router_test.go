package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/X1Kun/orion-live/internal/handler"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/gin-gonic/gin"
)

func TestLiveSessionRouteAuthenticationScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	liveSessions := &routerLiveSessionServiceStub{
		get: func(context.Context, uint64) (*model.LiveSession, error) {
			return &model.LiveSession{
				BaseModel: model.BaseModel{ID: 1},
				Status:    model.LiveSessionStatusScheduled,
			}, nil
		},
	}
	engine := New(
		handler.NewUserHandler(&routerUserServiceStub{}),
		handler.NewLiveSessionHandler(liveSessions),
		handler.NewHealthHandler(routerReadinessCheckerStub{}, 0),
		"test-secret-with-at-least-32-characters",
	)

	publicResponse := httptest.NewRecorder()
	engine.ServeHTTP(publicResponse, httptest.NewRequest(http.MethodGet, "/api/v1/live-sessions/1", nil))
	if publicResponse.Code != http.StatusOK {
		t.Fatalf("public GET status = %d, want %d; body = %s", publicResponse.Code, http.StatusOK, publicResponse.Body.String())
	}

	protectedRoutes := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/api/v1/live-sessions"},
		{method: http.MethodPost, path: "/api/v1/live-sessions/1/start"},
		{method: http.MethodPost, path: "/api/v1/live-sessions/1/end"},
	}
	for _, route := range protectedRoutes {
		response := httptest.NewRecorder()
		engine.ServeHTTP(response, httptest.NewRequest(route.method, route.path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Errorf("%s %s status = %d, want %d", route.method, route.path, response.Code, http.StatusUnauthorized)
		}
	}
}

type routerReadinessCheckerStub struct{}

func (routerReadinessCheckerStub) Ready(context.Context) error { return nil }

type routerUserServiceStub struct{}

func (*routerUserServiceStub) Register(context.Context, string, string) (*model.User, error) {
	return nil, nil
}

func (*routerUserServiceStub) Login(context.Context, string, string) (string, error) {
	return "", nil
}

type routerLiveSessionServiceStub struct {
	get func(context.Context, uint64) (*model.LiveSession, error)
}

func (*routerLiveSessionServiceStub) Create(context.Context, uint64, string, string) (*model.LiveSession, error) {
	return nil, nil
}

func (s *routerLiveSessionServiceStub) Get(ctx context.Context, id uint64) (*model.LiveSession, error) {
	return s.get(ctx, id)
}

func (*routerLiveSessionServiceStub) Start(context.Context, uint64, uint64) (*model.LiveSession, error) {
	return nil, nil
}

func (*routerLiveSessionServiceStub) End(context.Context, uint64, uint64) (*model.LiveSession, error) {
	return nil, nil
}
