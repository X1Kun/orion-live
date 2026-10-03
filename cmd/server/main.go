package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/X1Kun/orion-live/internal/admission"
	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/handler"
	"github.com/X1Kun/orion-live/internal/health"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/metrics"
	"github.com/X1Kun/orion-live/internal/outbox"
	"github.com/X1Kun/orion-live/internal/persistence"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/realtime"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/internal/router"
	"github.com/X1Kun/orion-live/internal/service"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/X1Kun/orion-live/pkg/logger"
	mysqlclient "github.com/X1Kun/orion-live/pkg/mysql"
	redisclient "github.com/X1Kun/orion-live/pkg/redis"
	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logger.Log.WithError(err).Fatal("invalid configuration")
	}
	logger.InitLogger(cfg.Environment)
	if cfg.Environment != "development" {
		gin.SetMode(gin.ReleaseMode)
	}

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), cfg.DependencyInitTimeout)
	defer cancelStartup()

	db, err := mysqlclient.Open(startupCtx, cfg.MySQL)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize mysql")
	}
	sqlDB, err := db.DB()
	if err != nil {
		logger.Log.WithError(err).Fatal("access mysql connection pool")
	}
	defer sqlDB.Close()
	metrics.RegisterInfrastructureCollectors(sqlDB, db)

	redis, err := redisclient.Open(startupCtx, cfg.Redis)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize redis")
	}
	defer redis.Close()

	rabbitMQ, err := rabbitclient.Open(startupCtx, cfg.RabbitMQ)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize rabbitmq")
	}
	defer rabbitMQ.Close()
	if err := rabbitclient.InitializeCoreTopology(startupCtx, rabbitMQ, cfg.Persistence); err != nil {
		logger.Log.WithError(err).Fatal("initialize rabbitmq topology")
	}
	outboxPublisher, err := rabbitclient.NewPublisher(startupCtx, rabbitMQ)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize Outbox publisher")
	}
	defer outboxPublisher.Close()
	chatPublisher, err := rabbitclient.NewPublisher(startupCtx, rabbitMQ)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize Chat publisher")
	}
	defer chatPublisher.Close()
	userRepo := repository.NewUserRepository(db)
	userService := service.NewUserService(userRepo, cfg.JWTSecret, cfg.AccessTokenTTL)
	liveSessionRepo := repository.NewLiveSessionRepository(db)
	liveSessionService := service.NewLiveSessionService(liveSessionRepo)
	chatLimiter := admission.NewChatLimiter(redis, cfg.Chat)
	chatService := service.NewChatService(chatLimiter, chatPublisher, cfg.Chat)
	chatRepo := repository.NewChatRepository(db)
	chatHistoryService := service.NewChatHistoryService(liveSessionService, chatRepo)
	outboxRepo := repository.NewOutboxRepository(db)
	instanceToken, err := messaging.NewCorrelationID()
	if err != nil {
		logger.Log.WithError(err).Fatal("generate instance identity")
	}
	hostname, err := os.Hostname()
	if err != nil {
		logger.Log.WithError(err).Fatal("read instance hostname")
	}
	webSocketHub, err := roomhub.NewHub(cfg.WebSocket.RoomBroadcastQueueCapacity)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize WebSocket hub")
	}
	realtimeSubscriber, err := realtime.Start(startupCtx, rabbitMQ, webSocketHub, cfg.RabbitMQ.RealtimePrefetch)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize realtime subscriber")
	}
	persistenceConsumer, err := persistence.StartConsumer(
		startupCtx, rabbitMQ, chatRepo, cfg.Persistence,
	)
	if err != nil {
		logger.Log.WithError(err).Fatal("initialize persistence consumer")
	}
	outboxRelay := outbox.StartRelay(outboxRepo, outboxPublisher, hostname+":"+instanceToken[:8], cfg.Outbox)
	checker := health.NewChecker(sqlDB, redis, rabbitMQ, realtimeSubscriber, persistenceConsumer)
	webSocketHandler := handler.NewWebSocketHandler(liveSessionService, chatService, webSocketHub, cfg.WebSocket)
	engine := router.New(
		handler.NewUserHandler(userService),
		handler.NewLiveSessionHandler(liveSessionService),
		handler.NewChatHandler(chatHistoryService),
		webSocketHandler,
		handler.NewHealthHandler(checker, time.Second),
		cfg.JWTSecret,
	)
	server := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           engine,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
		MaxHeaderBytes:    cfg.HTTP.MaxHeaderBytes,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Log.WithField("address", cfg.HTTP.Address).Info("api server started")
		serverErr <- server.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-signalCtx.Done():
		logger.Log.Info("shutdown signal received")
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Log.WithError(err).Fatal("api server stopped unexpectedly")
		}
	}

	checker.SetDraining()
	webSocketHandler.SetDraining()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ProcessShutdownTimeout)
	defer cancelShutdown()
	var shutdownWG sync.WaitGroup
	shutdownWG.Add(5)
	go func() {
		defer shutdownWG.Done()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Log.WithError(err).Error("HTTP shutdown did not complete")
			_ = server.Close()
		}
	}()
	go func() {
		defer shutdownWG.Done()
		if err := outboxRelay.Shutdown(shutdownCtx); err != nil {
			logger.Log.WithError(err).Error("Outbox Relay shutdown did not complete")
		}
	}()
	go func() {
		defer shutdownWG.Done()
		if err := webSocketHandler.Shutdown(shutdownCtx); err != nil {
			logger.Log.WithError(err).Error("WebSocket shutdown did not complete")
		}
	}()
	go func() {
		defer shutdownWG.Done()
		if err := realtimeSubscriber.Shutdown(shutdownCtx); err != nil {
			logger.Log.WithError(err).Error("realtime subscriber shutdown did not complete")
		}
	}()
	go func() {
		defer shutdownWG.Done()
		if err := persistenceConsumer.Shutdown(shutdownCtx); err != nil {
			logger.Log.WithError(err).Error("persistence consumer shutdown did not complete")
		}
	}()
	shutdownWG.Wait()
	logger.Log.Info("api server stopped")
}
