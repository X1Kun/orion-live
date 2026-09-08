package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/model"
	"gorm.io/gorm"
)

func TestLiveSessionLifecycle(t *testing.T) {
	repository := &liveSessionRepositoryFake{}
	service := NewLiveSessionService(repository)

	session, err := service.Create(context.Background(), 42, "  Launch Stream  ", "https://cdn.example.com/cover.jpg")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if session.ID == 0 || session.HostUserID != 42 || session.Title != "Launch Stream" {
		t.Fatalf("Create() session = %#v", session)
	}
	if session.CoverURL == nil || *session.CoverURL != "https://cdn.example.com/cover.jpg" {
		t.Fatalf("Create() cover URL = %v", session.CoverURL)
	}
	if session.Status != model.LiveSessionStatusScheduled {
		t.Fatalf("Create() status = %q, want %q", session.Status, model.LiveSessionStatusScheduled)
	}

	session, err = service.Start(context.Background(), session.ID, 42)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if session.Status != model.LiveSessionStatusLive || session.StartedAt == nil {
		t.Fatalf("Start() session = %#v", session)
	}

	session, err = service.End(context.Background(), session.ID, 42)
	if err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if session.Status != model.LiveSessionStatusEnded || session.EndedAt == nil {
		t.Fatalf("End() session = %#v", session)
	}
}

func TestCreateLiveSessionRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		title    string
		coverURL string
		want     error
	}{
		{name: "blank title", title: "   ", want: ErrInvalidLiveSessionTitle},
		{name: "long title", title: strings.Repeat("界", 256), want: ErrInvalidLiveSessionTitle},
		{name: "relative cover", title: "Stream", coverURL: "/cover.jpg", want: ErrInvalidLiveSessionCover},
		{name: "unsupported cover scheme", title: "Stream", coverURL: "ftp://example.com/cover.jpg", want: ErrInvalidLiveSessionCover},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &liveSessionRepositoryFake{}
			service := NewLiveSessionService(repository)
			_, err := service.Create(context.Background(), 42, tt.title, tt.coverURL)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Create() error = %v, want %v", err, tt.want)
			}
			if repository.session != nil {
				t.Fatal("invalid session was persisted")
			}
		})
	}
}

func TestLiveSessionTransitionsRejectInvalidRequests(t *testing.T) {
	tests := []struct {
		name      string
		session   *model.LiveSession
		hostID    uint64
		operation func(LiveSessionService, uint64, uint64) (*model.LiveSession, error)
		want      error
	}{
		{
			name:    "non-host start",
			session: &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusScheduled},
			hostID:  7,
			operation: func(s LiveSessionService, id, userID uint64) (*model.LiveSession, error) {
				return s.Start(context.Background(), id, userID)
			},
			want: ErrLiveSessionForbidden,
		},
		{
			name:    "start live session",
			session: &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusLive},
			hostID:  42,
			operation: func(s LiveSessionService, id, userID uint64) (*model.LiveSession, error) {
				return s.Start(context.Background(), id, userID)
			},
			want: ErrInvalidLiveSessionState,
		},
		{
			name:    "end scheduled session",
			session: &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusScheduled},
			hostID:  42,
			operation: func(s LiveSessionService, id, userID uint64) (*model.LiveSession, error) {
				return s.End(context.Background(), id, userID)
			},
			want: ErrInvalidLiveSessionState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &liveSessionRepositoryFake{session: cloneLiveSession(tt.session)}
			service := NewLiveSessionService(repository)
			_, err := tt.operation(service, tt.session.ID, tt.hostID)
			if !errors.Is(err, tt.want) {
				t.Fatalf("transition error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestLiveSessionTransitionDetectsConcurrentChange(t *testing.T) {
	repository := &liveSessionRepositoryFake{
		session:        &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusScheduled},
		forceStartMiss: true,
	}
	service := NewLiveSessionService(repository)

	_, err := service.Start(context.Background(), 1, 42)
	if !errors.Is(err, ErrInvalidLiveSessionState) {
		t.Fatalf("Start() error = %v, want %v", err, ErrInvalidLiveSessionState)
	}
}

func TestEndLiveSessionDetectsConcurrentChange(t *testing.T) {
	repository := &liveSessionRepositoryFake{
		session:      &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusLive},
		forceEndMiss: true,
	}
	service := NewLiveSessionService(repository)

	_, err := service.End(context.Background(), 1, 42)
	if !errors.Is(err, ErrInvalidLiveSessionState) {
		t.Fatalf("End() error = %v, want %v", err, ErrInvalidLiveSessionState)
	}
}

func TestStartLiveSessionRejectsHostWithAnotherLiveSession(t *testing.T) {
	repository := &liveSessionRepositoryFake{
		session:    &model.LiveSession{BaseModel: model.BaseModel{ID: 1}, HostUserID: 42, Status: model.LiveSessionStatusScheduled},
		startError: gorm.ErrDuplicatedKey,
	}
	service := NewLiveSessionService(repository)

	_, err := service.Start(context.Background(), 1, 42)
	if !errors.Is(err, ErrHostAlreadyLive) {
		t.Fatalf("Start() error = %v, want %v", err, ErrHostAlreadyLive)
	}
}

func TestGetLiveSessionMapsNotFound(t *testing.T) {
	service := NewLiveSessionService(&liveSessionRepositoryFake{})
	_, err := service.Get(context.Background(), 999)
	if !errors.Is(err, ErrLiveSessionNotFound) {
		t.Fatalf("Get() error = %v, want %v", err, ErrLiveSessionNotFound)
	}
}

type liveSessionRepositoryFake struct {
	session        *model.LiveSession
	forceStartMiss bool
	forceEndMiss   bool
	startError     error
}

func (r *liveSessionRepositoryFake) Create(_ context.Context, session *model.LiveSession) error {
	session.ID = 1
	session.CreatedAt = time.Now().UTC()
	session.UpdatedAt = session.CreatedAt
	r.session = cloneLiveSession(session)
	return nil
}

func (r *liveSessionRepositoryFake) FindByID(_ context.Context, id uint64) (*model.LiveSession, error) {
	if r.session == nil || r.session.ID != id {
		return nil, gorm.ErrRecordNotFound
	}
	return cloneLiveSession(r.session), nil
}

func (r *liveSessionRepositoryFake) Start(_ context.Context, id, hostUserID uint64) (bool, error) {
	if r.startError != nil {
		return false, r.startError
	}
	if r.forceStartMiss || r.session == nil || r.session.ID != id || r.session.HostUserID != hostUserID || r.session.Status != model.LiveSessionStatusScheduled {
		return false, nil
	}
	now := time.Now().UTC()
	r.session.Status = model.LiveSessionStatusLive
	r.session.StartedAt = &now
	r.session.UpdatedAt = now
	return true, nil
}

func (r *liveSessionRepositoryFake) End(_ context.Context, id, hostUserID uint64) (bool, error) {
	if r.forceEndMiss || r.session == nil || r.session.ID != id || r.session.HostUserID != hostUserID || r.session.Status != model.LiveSessionStatusLive {
		return false, nil
	}
	now := time.Now().UTC()
	r.session.Status = model.LiveSessionStatusEnded
	r.session.EndedAt = &now
	r.session.UpdatedAt = now
	return true, nil
}

func cloneLiveSession(session *model.LiveSession) *model.LiveSession {
	if session == nil {
		return nil
	}
	clone := *session
	return &clone
}
