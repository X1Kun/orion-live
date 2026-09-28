package handler

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/metrics"
	"github.com/X1Kun/orion-live/internal/middleware"
	"github.com/X1Kun/orion-live/internal/service"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/X1Kun/orion-live/pkg/logger"
	"github.com/gin-gonic/gin"
	gorilla "github.com/gorilla/websocket"
)

var (
	errWebSocketDraining         = errors.New("WebSocket server is shutting down")
	errWebSocketCapacityReached  = errors.New("WebSocket connection capacity reached")
	errWebSocketUserLimitReached = errors.New("WebSocket user connection limit reached")
)

type WebSocketHandler struct {
	sessions service.LiveSessionService
	chat     service.ChatService
	hub      *roomhub.Hub
	config   config.WebSocket
	upgrader gorilla.Upgrader

	lifecycleMu  sync.Mutex
	connections  sync.WaitGroup
	draining     bool
	active       int
	activeByUser map[uint64]int
}

func NewWebSocketHandler(sessions service.LiveSessionService, chat service.ChatService, hub *roomhub.Hub, cfg config.WebSocket) *WebSocketHandler {
	return &WebSocketHandler{
		sessions:     sessions,
		chat:         chat,
		hub:          hub,
		config:       cfg,
		upgrader:     gorilla.Upgrader{HandshakeTimeout: cfg.HandshakeTimeout},
		activeByUser: make(map[uint64]int),
	}
}

func (h *WebSocketHandler) Connect(c *gin.Context) {
	liveSessionID, ok := liveSessionID(c)
	if !ok {
		return
	}
	userID, ok := authenticatedUserID(c)
	if !ok {
		return
	}
	if err := h.sessions.AuthorizeJoin(c.Request.Context(), liveSessionID, userID); err != nil {
		handleLiveSessionError(c, err, "authorize WebSocket join")
		return
	}
	if err := h.admit(userID); err != nil {
		handleWebSocketAdmissionError(c, err)
		return
	}
	defer h.release(userID)

	client, err := roomhub.NewClient(h.config.ClientSendQueueCapacity)
	if err != nil {
		logger.Log.WithError(err).Error("create WebSocket client")
		sendError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
		return
	}
	connection, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Log.WithError(err).Debug("upgrade WebSocket connection")
		return
	}
	c.Set(middleware.WebSocketConnectionKey, true)
	if err := h.hub.Join(liveSessionID, client); err != nil {
		writeWebSocketClose(connection, gorilla.CloseTryAgainLater, "room is unavailable", h.config.WriteTimeout)
		_ = connection.Close()
		logger.Log.WithError(err).Warn("join WebSocket room")
		return
	}
	metrics.WebSocketConnections.Inc()
	defer metrics.WebSocketConnections.Dec()

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- h.writePump(connection, client)
		_ = connection.Close()
	}()

	readErr := h.readPump(c.Request.Context(), connection, client, liveSessionID, userID)
	if errors.Is(readErr, errWebSocketMessageTypeUnsupported) {
		writeWebSocketClose(connection, gorilla.CloseUnsupportedData, readErr.Error(), h.config.WriteTimeout)
		_ = connection.Close()
	} else if errors.Is(readErr, errWebSocketOutboundQueueFull) {
		writeWebSocketClose(connection, gorilla.CloseTryAgainLater, readErr.Error(), h.config.WriteTimeout)
		_ = connection.Close()
	}
	h.hub.Leave(liveSessionID, client)
	writeErr := <-writerDone
	_ = connection.Close()

	fields := logger.Log.WithField("live_session_id", liveSessionID).WithField("user_id", userID)
	if unexpectedWebSocketError(readErr) {
		fields.WithError(readErr).Debug("WebSocket reader stopped")
	}
	if unexpectedWebSocketError(writeErr) {
		fields.WithError(writeErr).Debug("WebSocket writer stopped")
	}
}

func (h *WebSocketHandler) Shutdown(ctx context.Context) error {
	h.SetDraining()

	hubErr := h.hub.Shutdown(ctx)
	connectionsDone := make(chan struct{})
	go func() {
		h.connections.Wait()
		close(connectionsDone)
	}()
	select {
	case <-connectionsDone:
		return hubErr
	case <-ctx.Done():
		return errors.Join(hubErr, ctx.Err())
	}
}

func (h *WebSocketHandler) SetDraining() {
	h.lifecycleMu.Lock()
	h.draining = true
	h.lifecycleMu.Unlock()
}

func (h *WebSocketHandler) admit(userID uint64) error {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.draining {
		return errWebSocketDraining
	}
	if h.active >= h.config.MaxConnections {
		return errWebSocketCapacityReached
	}
	if h.activeByUser[userID] >= h.config.MaxConnectionsPerUser {
		return errWebSocketUserLimitReached
	}
	h.active++
	h.activeByUser[userID]++
	h.connections.Add(1)
	return nil
}

func (h *WebSocketHandler) release(userID uint64) {
	h.lifecycleMu.Lock()
	h.active--
	h.activeByUser[userID]--
	if h.activeByUser[userID] == 0 {
		delete(h.activeByUser, userID)
	}
	h.lifecycleMu.Unlock()
	h.connections.Done()
}

func handleWebSocketAdmissionError(c *gin.Context, err error) {
	if errors.Is(err, errWebSocketUserLimitReached) {
		sendError(c, http.StatusTooManyRequests, "WEBSOCKET_CONNECTION_LIMIT", "user WebSocket connection limit reached")
		return
	}
	sendError(c, http.StatusServiceUnavailable, "WEBSOCKET_UNAVAILABLE", "WebSocket connections are temporarily unavailable")
}
