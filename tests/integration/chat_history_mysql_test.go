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

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/migrations"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestChatHistoryRepositoryUsesAscendingCursor(t *testing.T) {
	dsn := os.Getenv("ORION_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ORION_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	userResult, err := sqlDB.ExecContext(ctx, "INSERT INTO users(username, password_hash) VALUES (?, ?)", fmt.Sprintf("chat-history-%d", time.Now().UnixNano()), "not-used")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	userID, _ := userResult.LastInsertId()
	sessionResult, err := sqlDB.ExecContext(ctx, `INSERT INTO live_sessions(host_user_id, title, status, started_at) VALUES (?, 'History', 'LIVE', UTC_TIMESTAMP(6))`, userID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	sessionID, _ := sessionResult.LastInsertId()
	defer func() {
		_, _ = sqlDB.Exec("DELETE FROM consumer_inbox WHERE event_id IN (SELECT event_id FROM chat_messages WHERE live_session_id = ?)", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM chat_messages WHERE live_session_id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM live_sessions WHERE id = ?", sessionID)
		_, _ = sqlDB.Exec("DELETE FROM users WHERE id = ?", userID)
	}()

	repo := repository.NewChatRepository(db)
	seedChatHistoryMessage(t, ctx, repo, uint64(sessionID), uint64(userID), "018f47a2-8e31-4f10-8af0-2bdac5812520", "first")
	seedChatHistoryMessage(t, ctx, repo, uint64(sessionID), uint64(userID), "018f47a2-8e31-4f10-8af0-2bdac5812521", "second")
	firstPage, err := repo.ListAfter(ctx, uint64(sessionID), 0, 1)
	if err != nil {
		t.Fatalf("first ListAfter() error = %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].Content != "first" {
		t.Fatalf("unexpected first page: %#v", firstPage)
	}
	seedChatHistoryMessage(t, ctx, repo, uint64(sessionID), uint64(userID), "018f47a2-8e31-4f10-8af0-2bdac5812522", "third")
	secondPage, err := repo.ListAfter(ctx, uint64(sessionID), firstPage[0].ID, 10)
	if err != nil {
		t.Fatalf("second ListAfter() error = %v", err)
	}
	if len(secondPage) != 2 || secondPage[0].Content != "second" || secondPage[1].Content != "third" || secondPage[0].ID >= secondPage[1].ID {
		t.Fatalf("unexpected second page: %#v", secondPage)
	}
	empty, err := repo.ListAfter(ctx, uint64(sessionID), secondPage[1].ID, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty ListAfter() = %#v, %v", empty, err)
	}
}

func seedChatHistoryMessage(
	t *testing.T,
	ctx context.Context,
	repo repository.ChatPersistenceRepository,
	liveSessionID, userID uint64,
	messageID, content string,
) {
	t.Helper()
	event, err := messaging.NewChatMessageAcceptedEvent(liveSessionID, userID, messageID, content, time.Now().UTC())
	if err != nil {
		t.Fatalf("create Chat event: %v", err)
	}
	var payload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode Chat payload: %v", err)
	}
	if result, err := repo.Persist(ctx, "history-seed", event, payload); err != nil || result != repository.ChatPersisted {
		t.Fatalf("Persist() = %q, %v", result, err)
	}
}
