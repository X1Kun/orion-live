package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/service"
	"github.com/X1Kun/orion-live/pkg/logger"
	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	history service.ChatHistoryService
}

type chatMessageResponse struct {
	ID         uint64    `json:"id"`
	UserID     uint64    `json:"user_id"`
	MessageID  string    `json:"message_id"`
	Content    string    `json:"content"`
	AcceptedAt time.Time `json:"accepted_at"`
}

type chatHistoryPageResponse struct {
	NextCursor uint64 `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

func NewChatHandler(history service.ChatHistoryService) *ChatHandler {
	return &ChatHandler{history: history}
}

func (h *ChatHandler) History(c *gin.Context) {
	liveSessionID, ok := liveSessionID(c)
	if !ok {
		return
	}
	if _, ok := authenticatedUserID(c); !ok {
		return
	}
	afterID, limit, ok := chatHistoryQuery(c)
	if !ok {
		return
	}
	page, err := h.history.History(c.Request.Context(), liveSessionID, afterID, limit)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidChatHistoryLimit):
			sendError(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		case errors.Is(err, service.ErrLiveSessionNotFound):
			sendError(c, http.StatusNotFound, "LIVE_SESSION_NOT_FOUND", "live session not found")
		default:
			logger.Log.WithError(err).Error("get Chat history")
			sendError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
		}
		return
	}
	messages := make([]chatMessageResponse, len(page.Messages))
	for i := range page.Messages {
		messages[i] = newChatMessageResponse(page.Messages[i])
	}
	c.JSON(http.StatusOK, gin.H{
		"data": messages,
		"page": chatHistoryPageResponse{NextCursor: page.NextCursor, HasMore: page.HasMore},
	})
}

func chatHistoryQuery(c *gin.Context) (*uint64, int, bool) {
	var afterID *uint64
	if raw := c.Query("after_id"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			sendError(c, http.StatusBadRequest, "INVALID_REQUEST", "after_id must be a non-negative integer")
			return nil, 0, false
		}
		afterID = &parsed
	}
	var limit int
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			sendError(c, http.StatusBadRequest, "INVALID_REQUEST", "limit must be a positive integer")
			return nil, 0, false
		}
		limit = parsed
	}
	return afterID, limit, true
}

func newChatMessageResponse(message model.ChatMessage) chatMessageResponse {
	return chatMessageResponse{
		ID: message.ID, UserID: message.UserID, MessageID: message.MessageID,
		Content: message.Content, AcceptedAt: message.AcceptedAt.UTC(),
	}
}
