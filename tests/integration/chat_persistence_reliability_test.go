//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/persistence"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/migrations"
	_ "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestPersistenceConsumerRetriesThenPersists(t *testing.T) {
	fixture := newChatPersistenceFixture(t)
	failOnce := &failOnceChatRepository{delegate: fixture.repository}
	startPersistenceConsumer(t, fixture, failOnce)

	event := fixture.newEvent(t, "018f47a2-8e31-4f10-8af0-2bdac5812510", "retry succeeds")
	if err := fixture.publisher.Publish(fixture.ctx, event); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	waitForChatPersistence(t, func() bool {
		return fixture.countChat(event.EventID) == 1 && fixture.countInbox(event.EventID) == 1
	})
	calls, firstAttempt, secondAttempt := failOnce.snapshot()
	if calls != 2 {
		t.Fatalf("repository calls = %d, want 2", calls)
	}
	minimumDelay := persistenceIntegrationConfig().RetryMinDelay - 75*time.Millisecond
	if elapsed := secondAttempt.Sub(firstAttempt); elapsed < minimumDelay {
		t.Fatalf("retry delay = %s, want at least approximately %s", elapsed, persistenceIntegrationConfig().RetryMinDelay)
	}
	if messages := fixture.queueMessages(t, rabbitclient.PersistenceDeadLetterQueueName); messages != 0 {
		t.Fatalf("DLQ messages = %d, want 0", messages)
	}
}

func TestPersistenceConsumerDeadLettersConflictingMessage(t *testing.T) {
	fixture := newChatPersistenceFixture(t)
	dlq := consumePersistenceDLQ(t, fixture)
	startPersistenceConsumer(t, fixture, fixture.repository)

	messageID := "018f47a2-8e31-4f10-8af0-2bdac5812511"
	first := fixture.newEvent(t, messageID, "hello")
	firstPayload := decodeChatPayload(t, first)
	if result, err := fixture.repository.Persist(fixture.ctx, "conflict-seed", first, firstPayload); err != nil || result != repository.ChatPersisted {
		t.Fatalf("seed Persist() = %q, %v", result, err)
	}
	conflict := fixture.newEvent(t, messageID, "different")
	if err := fixture.publisher.Publish(fixture.ctx, conflict); err != nil {
		t.Fatalf("publish conflict: %v", err)
	}
	deadLetter := waitForDeadLetter(t, dlq, conflict.EventID)
	if deadLetter.MessageId != conflict.EventID {
		t.Fatalf("DLQ event ID = %q, want %q", deadLetter.MessageId, conflict.EventID)
	}
	var persisted model.ChatMessage
	if err := fixture.db.Where(
		"live_session_id = ? AND user_id = ? AND message_id = ?",
		fixture.sessionID,
		fixture.userID,
		messageID,
	).First(&persisted).Error; err != nil {
		t.Fatalf("read persisted Chat: %v", err)
	}
	if persisted.Content != "hello" {
		t.Fatalf("persisted content = %q, want hello", persisted.Content)
	}
	if count := fixture.countInbox(conflict.EventID); count != 0 {
		t.Fatalf("conflict Inbox count = %d, want 0", count)
	}
}

func TestPersistenceConsumerDeadLettersMalformedEvent(t *testing.T) {
	fixture := newChatPersistenceFixture(t)
	dlq := consumePersistenceDLQ(t, fixture)
	startPersistenceConsumer(t, fixture, fixture.repository)
	beforeChat := fixture.countAllChat()
	beforeInbox := fixture.countAllInbox()

	messageID := fmt.Sprintf("malformed-%d", time.Now().UnixNano())
	channel, err := fixture.rabbitMQ.Channel(fixture.ctx)
	if err != nil {
		t.Fatalf("open publish channel: %v", err)
	}
	defer channel.Close()
	if err := channel.PublishWithContext(
		fixture.ctx,
		rabbitclient.InteractionExchangeName,
		string(messaging.EventTypeChatMessageAccepted),
		true,
		false,
		amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent,
			MessageId: messageID, Type: string(messaging.EventTypeChatMessageAccepted), Body: []byte("{"),
		},
	); err != nil {
		t.Fatalf("publish malformed event: %v", err)
	}
	deadLetter := waitForDeadLetter(t, dlq, messageID)
	if string(deadLetter.Body) != "{" {
		t.Fatalf("DLQ body = %q, want original malformed body", deadLetter.Body)
	}
	if got := fixture.countAllChat(); got != beforeChat {
		t.Fatalf("Chat count = %d, want unchanged %d", got, beforeChat)
	}
	if got := fixture.countAllInbox(); got != beforeInbox {
		t.Fatalf("Inbox count = %d, want unchanged %d", got, beforeInbox)
	}
}

func TestChatRepositoryRollsBackInboxWhenMessageInsertFails(t *testing.T) {
	fixture := newChatPersistenceFixture(t)
	acceptedAt := time.Now().UTC().Truncate(time.Microsecond)
	event, err := messaging.NewChatMessageAcceptedEvent(
		fixture.sessionID+1_000_000,
		fixture.userID+1_000_000,
		"018f47a2-8e31-4f10-8af0-2bdac5812512",
		"missing foreign keys",
		acceptedAt,
	)
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	payload := decodeChatPayload(t, event)
	if _, err := fixture.repository.Persist(fixture.ctx, "rollback-test", event, payload); err == nil {
		t.Fatal("Persist() unexpectedly succeeded")
	}
	if count := fixture.countChat(event.EventID); count != 0 {
		t.Fatalf("Chat count = %d, want 0", count)
	}
	if count := fixture.countInbox(event.EventID); count != 0 {
		t.Fatalf("Inbox count = %d, want 0", count)
	}
}

type chatPersistenceFixture struct {
	ctx        context.Context
	sqlDB      *sql.DB
	db         *gorm.DB
	rabbitMQ   *rabbitclient.Client
	publisher  *rabbitclient.Publisher
	repository chatPersistenceAndHistoryRepository
	userID     uint64
	sessionID  uint64
}

type chatPersistenceAndHistoryRepository interface {
	repository.ChatPersistenceRepository
	repository.ChatHistoryRepository
}

func newChatPersistenceFixture(t *testing.T) *chatPersistenceFixture {
	t.Helper()
	dsn := os.Getenv("ORION_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ORION_TEST_MYSQL_DSN is not set")
	}
	rabbitCfg, ok := rabbitMQIntegrationConfig(t)
	if !ok {
		t.Skip("ORION_TEST_RABBITMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		cancel()
		t.Fatalf("open MySQL: %v", err)
	}
	if err := migrations.Up(ctx, sqlDB); err != nil {
		cancel()
		_ = sqlDB.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), TranslateError: true})
	if err != nil {
		cancel()
		_ = sqlDB.Close()
		t.Fatalf("open GORM: %v", err)
	}
	userResult, err := sqlDB.ExecContext(ctx, "INSERT INTO users(username, password_hash) VALUES (?, ?)", fmt.Sprintf("chat-reliability-%d", time.Now().UnixNano()), "not-used")
	if err != nil {
		cancel()
		_ = sqlDB.Close()
		t.Fatalf("insert user: %v", err)
	}
	userID, _ := userResult.LastInsertId()
	sessionResult, err := sqlDB.ExecContext(ctx, `INSERT INTO live_sessions(host_user_id, title, status, started_at) VALUES (?, 'Chat Reliability', 'LIVE', UTC_TIMESTAMP(6))`, userID)
	if err != nil {
		cancel()
		_ = sqlDB.Close()
		t.Fatalf("insert session: %v", err)
	}
	sessionID, _ := sessionResult.LastInsertId()
	rabbitMQ, err := rabbitclient.Open(ctx, rabbitCfg)
	if err != nil {
		cancel()
		_ = sqlDB.Close()
		t.Fatalf("open RabbitMQ: %v", err)
	}
	if err := rabbitclient.InitializeCoreTopology(ctx, rabbitMQ, persistenceIntegrationConfig()); err != nil {
		cancel()
		_ = rabbitMQ.Close()
		_ = sqlDB.Close()
		t.Fatalf("initialize topology: %v", err)
	}
	channel, err := rabbitMQ.Channel(ctx)
	if err != nil {
		cancel()
		_ = rabbitMQ.Close()
		_ = sqlDB.Close()
		t.Fatalf("open purge channel: %v", err)
	}
	_, _ = channel.QueuePurge(rabbitclient.PersistenceQueueName, false)
	_, _ = channel.QueuePurge(rabbitclient.PersistenceDeadLetterQueueName, false)
	_ = channel.Close()
	publisher, err := rabbitclient.NewPublisher(ctx, rabbitMQ)
	if err != nil {
		cancel()
		_ = rabbitMQ.Close()
		_ = sqlDB.Close()
		t.Fatalf("create Publisher: %v", err)
	}
	fixture := &chatPersistenceFixture{
		ctx: ctx, sqlDB: sqlDB, db: db, rabbitMQ: rabbitMQ, publisher: publisher,
		repository: repository.NewChatRepository(db), userID: uint64(userID), sessionID: uint64(sessionID),
	}
	t.Cleanup(func() {
		_ = publisher.Close()
		_ = rabbitMQ.Close()
		_, _ = sqlDB.Exec("DELETE FROM consumer_inbox WHERE event_id IN (SELECT event_id FROM chat_messages WHERE live_session_id = ?)", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM chat_messages WHERE live_session_id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM live_sessions WHERE id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM users WHERE id = ?", userID)
		_ = sqlDB.Close()
		cancel()
	})
	return fixture
}

func (f *chatPersistenceFixture) newEvent(t *testing.T, messageID, content string) messaging.Event {
	t.Helper()
	event, err := messaging.NewChatMessageAcceptedEvent(f.sessionID, f.userID, messageID, content, time.Now().UTC())
	if err != nil {
		t.Fatalf("create Chat event: %v", err)
	}
	return event
}

func (f *chatPersistenceFixture) countChat(eventID string) int64 {
	var count int64
	_ = f.db.Model(&model.ChatMessage{}).Where("event_id = ?", eventID).Count(&count).Error
	return count
}

func (f *chatPersistenceFixture) countInbox(eventID string) int64 {
	var count int64
	_ = f.db.Model(&model.ConsumerInbox{}).Where("event_id = ?", eventID).Count(&count).Error
	return count
}

func (f *chatPersistenceFixture) countAllChat() int64 {
	var count int64
	_ = f.db.Model(&model.ChatMessage{}).Count(&count).Error
	return count
}

func (f *chatPersistenceFixture) countAllInbox() int64 {
	var count int64
	_ = f.db.Model(&model.ConsumerInbox{}).Count(&count).Error
	return count
}

func (f *chatPersistenceFixture) queueMessages(t *testing.T, queue string) int {
	t.Helper()
	channel, err := f.rabbitMQ.Channel(f.ctx)
	if err != nil {
		t.Fatalf("open inspect channel: %v", err)
	}
	defer channel.Close()
	state, err := channel.QueueInspect(queue)
	if err != nil {
		t.Fatalf("inspect queue %q: %v", queue, err)
	}
	return state.Messages
}

func startPersistenceConsumer(t *testing.T, fixture *chatPersistenceFixture, repo repository.ChatPersistenceRepository) {
	t.Helper()
	consumer, err := persistence.StartConsumer(fixture.ctx, fixture.rabbitMQ, repo, persistenceIntegrationConfig())
	if err != nil {
		t.Fatalf("start Persistence Consumer: %v", err)
	}
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := consumer.Shutdown(shutdown); err != nil {
			t.Errorf("shutdown Persistence Consumer: %v", err)
		}
	})
}

func consumePersistenceDLQ(t *testing.T, fixture *chatPersistenceFixture) <-chan amqp.Delivery {
	t.Helper()
	channel, err := fixture.rabbitMQ.Channel(fixture.ctx)
	if err != nil {
		t.Fatalf("open DLQ channel: %v", err)
	}
	deliveries, err := channel.Consume(rabbitclient.PersistenceDeadLetterQueueName, "", true, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		t.Fatalf("consume DLQ: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return deliveries
}

func waitForDeadLetter(t *testing.T, deliveries <-chan amqp.Delivery, eventID string) amqp.Delivery {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case delivery := <-deliveries:
			if delivery.MessageId == eventID {
				return delivery
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for DLQ event %q", eventID)
		}
	}
}

func waitForChatPersistence(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Chat persistence state")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func decodeChatPayload(t *testing.T, event messaging.Event) messaging.ChatMessageAcceptedPayload {
	t.Helper()
	var payload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode Chat payload: %v", err)
	}
	return payload
}

type failOnceChatRepository struct {
	delegate repository.ChatPersistenceRepository
	mu       sync.Mutex
	calls    int
	first    time.Time
	second   time.Time
}

func (r *failOnceChatRepository) Persist(
	ctx context.Context,
	consumerName string,
	event messaging.Event,
	payload messaging.ChatMessageAcceptedPayload,
) (repository.ChatPersistenceResult, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	if call == 1 {
		r.first = time.Now()
	} else if call == 2 {
		r.second = time.Now()
	}
	r.mu.Unlock()
	if call == 1 {
		return "", errors.New("temporary repository failure")
	}
	return r.delegate.Persist(ctx, consumerName, event, payload)
}

func (r *failOnceChatRepository) snapshot() (int, time.Time, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.first, r.second
}
