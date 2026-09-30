//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/persistence"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/migrations"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestChatPersistenceIdempotencyAndConsumer(t *testing.T) {
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

	username := fmt.Sprintf("chat-user-%d", time.Now().UnixNano())
	userResult, err := sqlDB.ExecContext(ctx, "INSERT INTO users(username, password_hash) VALUES (?, ?)", username, "not-used")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	userID, _ := userResult.LastInsertId()
	sessionResult, err := sqlDB.ExecContext(ctx, `INSERT INTO live_sessions(host_user_id, title, status, started_at) VALUES (?, 'Chat', 'LIVE', UTC_TIMESTAMP(6))`, userID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	sessionID, _ := sessionResult.LastInsertId()
	defer func() {
		_, _ = sqlDB.Exec("DELETE FROM consumer_inbox WHERE consumer_name IN ('chat-persistence-v1', 'repository-test')")
		_, _ = sqlDB.Exec("DELETE FROM chat_messages WHERE live_session_id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM live_sessions WHERE id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM users WHERE id = ?", userID)
	}()

	repo := repository.NewChatRepository(db)
	acceptedAt := time.Now().UTC().Truncate(time.Microsecond)
	first, err := messaging.NewChatMessageAcceptedEvent(uint64(sessionID), uint64(userID), "018f47a2-8e31-4f10-8af0-2bdac5812501", "hello", acceptedAt)
	if err != nil {
		t.Fatalf("create first event: %v", err)
	}
	payload := messaging.ChatMessageAcceptedPayload{MessageID: "018f47a2-8e31-4f10-8af0-2bdac5812501", Content: "hello", AcceptedAt: acceptedAt}
	if result, err := repo.Persist(ctx, "repository-test", first, payload); err != nil || result != repository.ChatPersisted {
		t.Fatalf("first Persist() = %q, %v", result, err)
	}
	if result, err := repo.Persist(ctx, "repository-test", first, payload); err != nil || result != repository.ChatDuplicateEvent {
		t.Fatalf("duplicate event Persist() = %q, %v", result, err)
	}
	second, _ := messaging.NewChatMessageAcceptedEvent(uint64(sessionID), uint64(userID), payload.MessageID, payload.Content, acceptedAt.Add(time.Millisecond))
	if result, err := repo.Persist(ctx, "repository-test", second, messaging.ChatMessageAcceptedPayload{MessageID: payload.MessageID, Content: payload.Content, AcceptedAt: acceptedAt.Add(time.Millisecond)}); err != nil || result != repository.ChatDuplicateMessage {
		t.Fatalf("duplicate message Persist() = %q, %v", result, err)
	}
	conflict, _ := messaging.NewChatMessageAcceptedEvent(uint64(sessionID), uint64(userID), payload.MessageID, "different", acceptedAt.Add(2*time.Millisecond))
	if result, err := repo.Persist(ctx, "repository-test", conflict, messaging.ChatMessageAcceptedPayload{MessageID: payload.MessageID, Content: "different", AcceptedAt: acceptedAt.Add(2 * time.Millisecond)}); err != nil || result != repository.ChatConflictingMessage {
		t.Fatalf("conflicting message Persist() = %q, %v", result, err)
	}
	var conflictInboxCount int64
	if err := db.Model(&model.ConsumerInbox{}).Where("consumer_name = ? AND event_id = ?", "repository-test", conflict.EventID).Count(&conflictInboxCount).Error; err != nil || conflictInboxCount != 0 {
		t.Fatalf("conflict Inbox count = %d, error = %v; want 0", conflictInboxCount, err)
	}

	rabbitMQ, err := rabbitclient.Open(ctx, rabbitCfg)
	if err != nil {
		t.Fatalf("open RabbitMQ: %v", err)
	}
	defer rabbitMQ.Close()
	if err := rabbitclient.InitializeCoreTopology(ctx, rabbitMQ, persistenceIntegrationConfig()); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	channel, err := rabbitMQ.Channel(ctx)
	if err != nil {
		t.Fatalf("open purge channel: %v", err)
	}
	_, _ = channel.QueuePurge(rabbitclient.PersistenceQueueName, false)
	_ = channel.Close()
	consumer, err := persistence.StartConsumer(ctx, rabbitMQ, repo, persistenceIntegrationConfig())
	if err != nil {
		t.Fatalf("start persistence Subscriber: %v", err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = consumer.Shutdown(shutdown)
	}()
	publisher, err := rabbitclient.NewPublisher(ctx, rabbitMQ)
	if err != nil {
		t.Fatalf("create Publisher: %v", err)
	}
	defer publisher.Close()
	consumerEvent, err := messaging.NewChatMessageAcceptedEvent(uint64(sessionID), uint64(userID), "018f47a2-8e31-4f10-8af0-2bdac5812502", "from consumer", time.Now().UTC())
	if err != nil {
		t.Fatalf("create consumer event: %v", err)
	}
	if err := publisher.Publish(ctx, consumerEvent); err != nil {
		t.Fatalf("publish consumer event: %v", err)
	}
	if err := publisher.Publish(ctx, consumerEvent); err != nil {
		t.Fatalf("republish consumer event: %v", err)
	}
	waitForOutbox(t, 5*time.Second, func() bool {
		var messages, inbox int64
		messageErr := db.Model(&model.ChatMessage{}).Where("event_id = ?", consumerEvent.EventID).Count(&messages).Error
		inboxErr := db.Model(&model.ConsumerInbox{}).Where("consumer_name = ? AND event_id = ?", "chat-persistence-v1", consumerEvent.EventID).Count(&inbox).Error
		return messageErr == nil && inboxErr == nil && messages == 1 && inbox == 1
	})
}
