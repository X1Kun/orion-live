//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/outbox"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/realtime"
	"github.com/X1Kun/orion-live/internal/repository"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
	"github.com/X1Kun/orion-live/migrations"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestLiveSessionEndedOutboxEndToEnd(t *testing.T) {
	dsn := os.Getenv("ORION_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ORION_TEST_MYSQL_DSN is not set")
	}
	rabbitCfg, ok := rabbitMQIntegrationConfig(t)
	if !ok {
		t.Skip("ORION_TEST_RABBITMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	defer sqlDB.Close()
	if err := migrations.Up(ctx, sqlDB); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), TranslateError: true})
	if err != nil {
		t.Fatalf("open GORM: %v", err)
	}

	username := fmt.Sprintf("outbox-host-%d", time.Now().UnixNano())
	result, err := sqlDB.ExecContext(ctx, "INSERT INTO users(username, password_hash) VALUES (?, ?)", username, "not-used")
	if err != nil {
		t.Fatalf("insert host: %v", err)
	}
	hostID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read host ID: %v", err)
	}
	defer func() {
		_, _ = sqlDB.Exec("DELETE FROM outbox_events WHERE event_type = 'live_session.ended'")
		_, _ = sqlDB.Exec("DELETE FROM live_sessions WHERE host_user_id = ?", hostID)
		_, _ = sqlDB.Exec("DELETE FROM users WHERE id = ?", hostID)
	}()

	sessions := repository.NewLiveSessionRepository(db)
	session := &model.LiveSession{HostUserID: uint64(hostID), Title: "Outbox", Status: model.LiveSessionStatusScheduled}
	if err := sessions.Create(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if updated, err := sessions.Start(ctx, session.ID, uint64(hostID)); err != nil || !updated {
		t.Fatalf("start session: %v %v", updated, err)
	}

	rabbitMQ, err := rabbitclient.Open(ctx, rabbitCfg)
	if err != nil {
		t.Fatalf("open RabbitMQ: %v", err)
	}
	defer rabbitMQ.Close()
	if err := rabbitclient.InitializeCoreTopology(ctx, rabbitMQ, persistenceIntegrationConfig()); err != nil {
		t.Fatalf("topology: %v", err)
	}
	hub, err := roomhub.NewHub(8)
	if err != nil {
		t.Fatalf("create Hub: %v", err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = hub.Shutdown(shutdown)
	}()
	roomClient, err := roomhub.NewClient(8)
	if err != nil {
		t.Fatalf("create Client: %v", err)
	}
	if err := hub.Join(session.ID, roomClient); err != nil {
		t.Fatalf("join Hub: %v", err)
	}
	subscriber, err := realtime.Start(ctx, rabbitMQ, hub, 8)
	if err != nil {
		t.Fatalf("start Subscriber: %v", err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = subscriber.Shutdown(shutdown)
	}()
	publisher, err := rabbitclient.NewPublisher(ctx, rabbitMQ)
	if err != nil {
		t.Fatalf("create Publisher: %v", err)
	}
	defer publisher.Close()
	relayCfg := config.Outbox{PollInterval: 10 * time.Millisecond, BatchSize: 10, LeaseDuration: time.Second, MaxAttempts: 3, PublishTimeout: time.Second, RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 100 * time.Millisecond}
	relay := outbox.StartRelay(repository.NewOutboxRepository(db), publisher, "integration-relay", relayCfg)
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = relay.Shutdown(shutdown)
	}()

	ended, updated, err := sessions.End(ctx, session.ID, uint64(hostID), "end-to-end-correlation")
	if err != nil || !updated || ended.EndedAt == nil {
		t.Fatalf("end session: %#v %v %v", ended, updated, err)
	}
	select {
	case body := <-roomClient.Outbound():
		var event messaging.Event
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if event.EventType != messaging.EventTypeLiveSessionEnded {
			t.Fatalf("event type = %q", event.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for live_session.ended")
	}
	if hub.SendAllowed(session.ID) {
		t.Fatal("ended session send gate remained open")
	}
	waitForOutbox(t, 5*time.Second, func() bool {
		var count int64
		return db.Model(&model.OutboxEvent{}).Where("event_type = ? AND status = ?", messaging.EventTypeLiveSessionEnded, model.OutboxStatusPublished).Count(&count).Error == nil && count == 1
	})
}

func waitForOutbox(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Outbox state")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
