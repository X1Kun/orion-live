package service

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/repository"
	"gorm.io/gorm"
)

var (
	ErrLiveSessionNotFound     = errors.New("live session not found")
	ErrLiveSessionForbidden    = errors.New("only the host can perform this operation")
	ErrInvalidLiveSessionState = errors.New("live session cannot perform this state transition")
	ErrHostAlreadyLive         = errors.New("host already has a live session")
	ErrInvalidLiveSessionTitle = errors.New("title must contain 1 to 255 characters")
	ErrInvalidLiveSessionCover = errors.New("cover_url must be an absolute HTTP(S) URL no longer than 2048 characters")
)

type LiveSessionService interface {
	Create(ctx context.Context, hostUserID uint64, title, coverURL string) (*model.LiveSession, error)
	Get(ctx context.Context, id uint64) (*model.LiveSession, error)
	Start(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error)
	End(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error)
}

type liveSessionService struct {
	sessions repository.LiveSessionRepository
}

func NewLiveSessionService(sessions repository.LiveSessionRepository) LiveSessionService {
	return &liveSessionService{sessions: sessions}
}

func (s *liveSessionService) Create(ctx context.Context, hostUserID uint64, title, coverURL string) (*model.LiveSession, error) {
	title = strings.TrimSpace(title)
	if count := utf8.RuneCountInString(title); count < 1 || count > 255 {
		return nil, ErrInvalidLiveSessionTitle
	}

	cover, err := normalizeCoverURL(coverURL)
	if err != nil {
		return nil, err
	}
	session := &model.LiveSession{
		HostUserID: hostUserID,
		Title:      title,
		CoverURL:   cover,
		Status:     model.LiveSessionStatusScheduled,
	}
	if err := s.sessions.Create(ctx, session); err != nil {
		return nil, err
	}
	return session, nil
}

func (s *liveSessionService) Get(ctx context.Context, id uint64) (*model.LiveSession, error) {
	session, err := s.sessions.FindByID(ctx, id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrLiveSessionNotFound
	}
	return session, err
}

func (s *liveSessionService) Start(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error) {
	session, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if session.HostUserID != hostUserID {
		return nil, ErrLiveSessionForbidden
	}
	if session.Status != model.LiveSessionStatusScheduled {
		return nil, ErrInvalidLiveSessionState
	}

	updated, err := s.sessions.Start(ctx, id, hostUserID)
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return nil, ErrHostAlreadyLive
	}
	if err != nil {
		return nil, err
	}
	if !updated {
		return nil, ErrInvalidLiveSessionState
	}
	return s.Get(ctx, id)
}

func (s *liveSessionService) End(ctx context.Context, id, hostUserID uint64) (*model.LiveSession, error) {
	session, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if session.HostUserID != hostUserID {
		return nil, ErrLiveSessionForbidden
	}
	if session.Status != model.LiveSessionStatusLive {
		return nil, ErrInvalidLiveSessionState
	}

	updated, err := s.sessions.End(ctx, id, hostUserID)
	if err != nil {
		return nil, err
	}
	if !updated {
		return nil, ErrInvalidLiveSessionState
	}
	return s.Get(ctx, id)
}

func normalizeCoverURL(value string) (*string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if len(value) > 2048 {
		return nil, ErrInvalidLiveSessionCover
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, ErrInvalidLiveSessionCover
	}
	return &value, nil
}
