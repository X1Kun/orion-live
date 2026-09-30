//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/model"
	"github.com/X1Kun/orion-live/internal/repository"
	"github.com/X1Kun/orion-live/migrations"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestLiveSessionMigrationAndRepository(t *testing.T) {
	dsn := os.Getenv("ORION_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ORION_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	defer sqlDB.Close()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping MySQL: %v", err)
	}

	if err := migrations.Up(ctx, sqlDB); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := migrations.Up(ctx, sqlDB); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	assertMigrationVersions(t, ctx, sqlDB, 1, 2, 3, 4)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:         logger.Default.LogMode(logger.Silent),
		TranslateError: true,
	})
	if err != nil {
		t.Fatalf("open GORM: %v", err)
	}

	username := fmt.Sprintf("integration-host-%d", time.Now().UnixNano())
	result, err := sqlDB.ExecContext(ctx, "INSERT INTO users(username, password_hash) VALUES (?, ?)", username, "not-used-by-this-test")
	if err != nil {
		t.Fatalf("insert host: %v", err)
	}
	hostUserID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read host ID: %v", err)
	}
	defer func() {
		_, _ = sqlDB.Exec("DELETE FROM outbox_events WHERE event_type = ?", "live_session.ended")
		_, _ = sqlDB.Exec("DELETE FROM live_sessions WHERE host_user_id = ?", hostUserID)
		_, _ = sqlDB.Exec("DELETE FROM users WHERE id = ?", hostUserID)
	}()

	sessions := repository.NewLiveSessionRepository(db)
	first := &model.LiveSession{HostUserID: uint64(hostUserID), Title: "First", Status: model.LiveSessionStatusScheduled}
	second := &model.LiveSession{HostUserID: uint64(hostUserID), Title: "Second", Status: model.LiveSessionStatusScheduled}
	if err := sessions.Create(ctx, first); err != nil {
		t.Fatalf("create first session: %v", err)
	}
	if err := sessions.Create(ctx, second); err != nil {
		t.Fatalf("create second session: %v", err)
	}

	updated, err := sessions.Start(ctx, first.ID, uint64(hostUserID))
	if err != nil || !updated {
		t.Fatalf("start first session: updated = %v, error = %v", updated, err)
	}
	updated, err = sessions.Start(ctx, second.ID, uint64(hostUserID))
	if updated || !errors.Is(err, gorm.ErrDuplicatedKey) {
		t.Fatalf("start second session: updated = %v, error = %v; want duplicated key", updated, err)
	}

	ended, updated, err := sessions.End(ctx, first.ID, uint64(hostUserID), "integration-end")
	if err != nil || !updated {
		t.Fatalf("end first session: updated = %v, error = %v", updated, err)
	}
	if ended.Status != model.LiveSessionStatusEnded || ended.EndedAt == nil {
		t.Fatalf("ended session = %#v", ended)
	}
	outboxRepository := repository.NewOutboxRepository(db)
	claimed, err := outboxRepository.ClaimBatch(ctx, "integration-publisher", 10, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim Outbox: count = %d, error = %v", len(claimed), err)
	}
	if claimed[0].AttemptCount != 1 || claimed[0].ClaimToken == nil {
		t.Fatalf("claimed Outbox = %#v", claimed[0])
	}
	if updated, err := outboxRepository.MarkPublished(ctx, claimed[0].EventID, "stale-token"); err != nil || updated {
		t.Fatalf("stale fencing update = %v, error = %v", updated, err)
	}
	if updated, err := outboxRepository.MarkPublished(ctx, claimed[0].EventID, *claimed[0].ClaimToken); err != nil || !updated {
		t.Fatalf("current fencing update = %v, error = %v", updated, err)
	}
	_, updated, err = sessions.End(ctx, first.ID, uint64(hostUserID), "integration-repeat-end")
	if err != nil || updated {
		t.Fatalf("repeat end first session: updated = %v, error = %v", updated, err)
	}
	updated, err = sessions.Start(ctx, second.ID, uint64(hostUserID))
	if err != nil || !updated {
		t.Fatalf("start second session after first ended: updated = %v, error = %v", updated, err)
	}

	stored, err := sessions.FindByID(ctx, second.ID)
	if err != nil {
		t.Fatalf("find second session: %v", err)
	}
	if stored.Status != model.LiveSessionStatusLive || stored.StartedAt == nil || stored.EndedAt != nil {
		t.Fatalf("stored second session = %#v", stored)
	}

	collisionEvent, err := messaging.NewLiveSessionEndedEvent(second.ID, uint64(hostUserID), time.Now().UTC(), "collision")
	if err != nil {
		t.Fatalf("create collision event: %v", err)
	}
	collisionBody, err := collisionEvent.Marshal()
	if err != nil {
		t.Fatalf("marshal collision event: %v", err)
	}
	if err := db.Create(&model.OutboxEvent{
		EventID: collisionEvent.EventID, EventType: string(collisionEvent.EventType), SchemaVersion: 1,
		Payload: collisionBody, Status: model.OutboxStatusPending, AvailableAt: time.Now().UTC(),
	}).Error; err != nil {
		t.Fatalf("insert collision Outbox: %v", err)
	}
	if _, updated, err := sessions.End(ctx, second.ID, uint64(hostUserID), "must-rollback"); err == nil || updated {
		t.Fatalf("End with duplicate Outbox: updated = %v, error = %v", updated, err)
	}
	stored, err = sessions.FindByID(ctx, second.ID)
	if err != nil || stored.Status != model.LiveSessionStatusLive || stored.EndedAt != nil {
		t.Fatalf("End transaction did not roll back: session = %#v, error = %v", stored, err)
	}

	firstClaim, err := outboxRepository.ClaimBatch(ctx, "publisher-one", 1, 20*time.Millisecond)
	if err != nil || len(firstClaim) != 1 {
		t.Fatalf("first lease claim: %d %v", len(firstClaim), err)
	}
	time.Sleep(50 * time.Millisecond)
	secondClaim, err := outboxRepository.ClaimBatch(ctx, "publisher-two", 1, time.Minute)
	if err != nil || len(secondClaim) != 1 {
		t.Fatalf("expired lease reclaim: %d %v", len(secondClaim), err)
	}
	if updated, err := outboxRepository.MarkFailed(ctx, secondClaim[0].EventID, *firstClaim[0].ClaimToken, errors.New("stale")); err != nil || updated {
		t.Fatalf("stale lease update = %v, error = %v", updated, err)
	}
	if updated, err := outboxRepository.MarkFailed(ctx, secondClaim[0].EventID, *secondClaim[0].ClaimToken, errors.New("current")); err != nil || !updated {
		t.Fatalf("current lease update = %v, error = %v", updated, err)
	}
}

func assertMigrationVersions(t *testing.T, ctx context.Context, db *sql.DB, versions ...uint64) {
	t.Helper()
	for _, version := range versions {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&count); err != nil {
			t.Fatalf("query migration version %d: %v", version, err)
		}
		if count != 1 {
			t.Fatalf("migration version %d count = %d, want 1", version, count)
		}
	}
}
