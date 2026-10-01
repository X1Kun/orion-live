//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	"github.com/X1Kun/orion-live/internal/persistence"
	"github.com/X1Kun/orion-live/internal/realtime"
	"github.com/X1Kun/orion-live/internal/repository"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
)

func TestChatReconnectRecoveryMergesRealtimeAndHistory(t *testing.T) {
	fixture := newChatPersistenceFixture(t)
	initialConsumer, err := persistence.StartConsumer(fixture.ctx, fixture.rabbitMQ, fixture.repository, persistenceIntegrationConfig())
	if err != nil {
		t.Fatalf("start initial Persistence Consumer: %v", err)
	}
	beforeJoin := fixture.newEvent(t, "018f47a2-8e31-4f10-8af0-2bdac5812530", "before join")
	if err := fixture.publisher.Publish(fixture.ctx, beforeJoin); err != nil {
		t.Fatalf("publish before-join event: %v", err)
	}
	waitForChatPersistence(t, func() bool { return fixture.countChat(beforeJoin.EventID) == 1 })
	shutdownComponent(t, initialConsumer.Shutdown)

	hub, err := roomhub.NewHub(8)
	if err != nil {
		t.Fatalf("create Hub: %v", err)
	}
	client, err := roomhub.NewClient(8)
	if err != nil {
		t.Fatalf("create room Client: %v", err)
	}
	if err := hub.Join(fixture.sessionID, client); err != nil {
		t.Fatalf("join room: %v", err)
	}
	realtimeSubscriber, err := realtime.Start(fixture.ctx, fixture.rabbitMQ, hub, 8)
	if err != nil {
		t.Fatalf("start Realtime Subscriber: %v", err)
	}
	t.Cleanup(func() {
		shutdownComponent(t, realtimeSubscriber.Shutdown)
		shutdownComponent(t, hub.Shutdown)
	})

	blockingRepository := newBlockingOnceChatRepository(fixture.repository)
	recoveryConsumer, err := persistence.StartConsumer(fixture.ctx, fixture.rabbitMQ, blockingRepository, persistenceIntegrationConfig())
	if err != nil {
		t.Fatalf("start recovery Persistence Consumer: %v", err)
	}
	t.Cleanup(func() { shutdownComponent(t, recoveryConsumer.Shutdown) })
	afterJoin := fixture.newEvent(t, "018f47a2-8e31-4f10-8af0-2bdac5812531", "after join")
	if err := fixture.publisher.Publish(fixture.ctx, afterJoin); err != nil {
		t.Fatalf("publish after-join event: %v", err)
	}

	var realtimeEvent messaging.Event
	select {
	case body := <-client.Outbound():
		if err := json.Unmarshal(body, &realtimeEvent); err != nil {
			t.Fatalf("decode realtime event: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for realtime event")
	}
	if realtimeEvent.EventID != afterJoin.EventID {
		t.Fatalf("realtime event ID = %q, want %q", realtimeEvent.EventID, afterJoin.EventID)
	}
	select {
	case <-blockingRepository.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Persistence Consumer did not reach controlled failure")
	}

	initialHistory, err := fixture.repository.ListAfter(fixture.ctx, fixture.sessionID, 0, 10)
	if err != nil {
		t.Fatalf("read initial History: %v", err)
	}
	if len(initialHistory) != 1 || initialHistory[0].EventID != beforeJoin.EventID {
		t.Fatalf("initial History = %#v, want only before-join event", initialHistory)
	}
	close(blockingRepository.release)
	waitForChatPersistence(t, func() bool { return fixture.countChat(afterJoin.EventID) == 1 })
	recoveredHistory, err := fixture.repository.ListAfter(fixture.ctx, fixture.sessionID, initialHistory[0].ID, 10)
	if err != nil {
		t.Fatalf("read recovered History: %v", err)
	}
	if len(recoveredHistory) != 1 || recoveredHistory[0].EventID != afterJoin.EventID {
		t.Fatalf("recovered History = %#v, want after-join event", recoveredHistory)
	}

	merged := make(map[string]struct{})
	merged[chatMessageKey(initialHistory[0].LiveSessionID, initialHistory[0].UserID, initialHistory[0].MessageID)] = struct{}{}
	var realtimePayload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(realtimeEvent.Payload, &realtimePayload); err != nil {
		t.Fatalf("decode realtime payload: %v", err)
	}
	merged[chatMessageKey(realtimeEvent.LiveSessionID, realtimeEvent.UserID, realtimePayload.MessageID)] = struct{}{}
	merged[chatMessageKey(recoveredHistory[0].LiveSessionID, recoveredHistory[0].UserID, recoveredHistory[0].MessageID)] = struct{}{}
	if len(merged) != 2 {
		t.Fatalf("merged logical messages = %d, want 2", len(merged))
	}
}

func chatMessageKey(liveSessionID, userID uint64, messageID string) string {
	return fmt.Sprintf("%d:%d:%s", liveSessionID, userID, messageID)
}

func shutdownComponent(t *testing.T, shutdown func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown component: %v", err)
	}
}

type blockingOnceChatRepository struct {
	delegate repository.ChatPersistenceRepository
	failed   atomic.Bool
	entered  chan struct{}
	release  chan struct{}
}

func newBlockingOnceChatRepository(delegate repository.ChatPersistenceRepository) *blockingOnceChatRepository {
	return &blockingOnceChatRepository{delegate: delegate, entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *blockingOnceChatRepository) Persist(
	ctx context.Context,
	consumerName string,
	event messaging.Event,
	payload messaging.ChatMessageAcceptedPayload,
) (repository.ChatPersistenceResult, error) {
	if r.failed.CompareAndSwap(false, true) {
		close(r.entered)
		select {
		case <-r.release:
			return "", errors.New("controlled persistence failure")
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return r.delegate.Persist(ctx, consumerName, event, payload)
}
