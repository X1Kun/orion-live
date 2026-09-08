package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/X1Kun/orion-live/internal/middleware"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/service"
	"github.com/X1Kun/orion-live/pkg/logger"
	"github.com/gin-gonic/gin"
)

type LiveSessionHandler struct {
	sessions service.LiveSessionService
}

type liveSessionTransition func(context.Context, uint64, uint64) (*model.LiveSession, error)

type createLiveSessionRequest struct {
	Title    string `json:"title" binding:"required"`
	CoverURL string `json:"cover_url"`
}

type liveSessionResponse struct {
	ID         uint64                  `json:"id"`
	HostUserID uint64                  `json:"host_user_id"`
	Title      string                  `json:"title"`
	CoverURL   *string                 `json:"cover_url"`
	Status     model.LiveSessionStatus `json:"status"`
	StartedAt  *time.Time              `json:"started_at"`
	EndedAt    *time.Time              `json:"ended_at"`
	CreatedAt  time.Time               `json:"created_at"`
	UpdatedAt  time.Time               `json:"updated_at"`
}

func NewLiveSessionHandler(sessions service.LiveSessionService) *LiveSessionHandler {
	return &LiveSessionHandler{sessions: sessions}
}

func (h *LiveSessionHandler) Create(c *gin.Context) {
	var request createLiveSessionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		sendError(c, http.StatusBadRequest, "INVALID_REQUEST", "title is required")
		return
	}

	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	session, err := h.sessions.Create(c.Request.Context(), userID, request.Title, request.CoverURL)
	if err != nil {
		handleLiveSessionError(c, err, "create live session")
		return
	}
	c.JSON(http.StatusCreated, gin.H{"data": newLiveSessionResponse(session)})
}

func (h *LiveSessionHandler) Get(c *gin.Context) {
	id, ok := liveSessionID(c)
	if !ok {
		return
	}
	session, err := h.sessions.Get(c.Request.Context(), id)
	if err != nil {
		handleLiveSessionError(c, err, "get live session")
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": newLiveSessionResponse(session)})
}

func (h *LiveSessionHandler) Start(c *gin.Context) {
	h.transition(c, "start live session", h.sessions.Start)
}

func (h *LiveSessionHandler) End(c *gin.Context) {
	h.transition(c, "end live session", h.sessions.End)
}

func (h *LiveSessionHandler) transition(c *gin.Context, operation string, transition liveSessionTransition) {
	id, ok := liveSessionID(c)
	if !ok {
		return
	}
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}

	session, err := transition(c.Request.Context(), id, userID)
	if err != nil {
		handleLiveSessionError(c, err, operation)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": newLiveSessionResponse(session)})
}

func liveSessionID(c *gin.Context) (uint64, bool) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		sendError(c, http.StatusBadRequest, "INVALID_REQUEST", "live session id must be a positive integer")
		return 0, false
	}
	return id, true
}

func authenticatedUserID(c *gin.Context) (uint64, bool) {
	value, exists := c.Get(middleware.UserIDKey)
	userID, ok := value.(uint64)
	if !exists || !ok || userID == 0 {
		sendError(c, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
		return 0, false
	}
	return userID, true
}

func handleLiveSessionError(c *gin.Context, err error, operation string) {
	switch {
	case errors.Is(err, service.ErrInvalidLiveSessionTitle), errors.Is(err, service.ErrInvalidLiveSessionCover):
		sendError(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, service.ErrLiveSessionNotFound):
		sendError(c, http.StatusNotFound, "LIVE_SESSION_NOT_FOUND", "live session not found")
	case errors.Is(err, service.ErrLiveSessionForbidden):
		sendError(c, http.StatusForbidden, "FORBIDDEN", "only the host can perform this operation")
	case errors.Is(err, service.ErrInvalidLiveSessionState):
		sendError(c, http.StatusConflict, "INVALID_STATE_TRANSITION", "live session cannot perform this state transition")
	case errors.Is(err, service.ErrHostAlreadyLive):
		sendError(c, http.StatusConflict, "HOST_ALREADY_LIVE", "host already has a live session")
	default:
		logger.Log.WithError(err).Error(operation)
		sendError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
	}
}

func newLiveSessionResponse(session *model.LiveSession) liveSessionResponse {
	return liveSessionResponse{
		ID:         session.ID,
		HostUserID: session.HostUserID,
		Title:      session.Title,
		CoverURL:   session.CoverURL,
		Status:     session.Status,
		StartedAt:  utcTimePointer(session.StartedAt),
		EndedAt:    utcTimePointer(session.EndedAt),
		CreatedAt:  session.CreatedAt.UTC(),
		UpdatedAt:  session.UpdatedAt.UTC(),
	}
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}
