//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	rabbitclient "github.com/X1Kun/orion-live/internal/rabbitmq"
	"github.com/X1Kun/orion-live/internal/realtime"
	roomhub "github.com/X1Kun/orion-live/internal/websocket"
)

func TestRabbitMQTopologyPublisherAndReconnect(t *testing.T) {
	cfg, ok := rabbitMQIntegrationConfig(t)
	if !ok {
		t.Skip("ORION_TEST_RABBITMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := rabbitclient.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open RabbitMQ client: %v", err)
	}
	defer client.Close()
	if err := rabbitclient.InitializeCoreTopology(ctx, client, rabbitclient.DefaultPersistenceRetryDelay); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	if err := rabbitclient.InitializeCoreTopology(ctx, client, rabbitclient.DefaultPersistenceRetryDelay); err != nil {
		t.Fatalf("reinitialize topology: %v", err)
	}
	purgePersistenceQueue(t, ctx, client)

	publisher, err := rabbitclient.NewPublisher(ctx, client)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	defer publisher.Close()

	first := integrationChatEvent("event-before-reconnect")
	if err := publisher.Publish(ctx, first); err != nil {
		t.Fatalf("publish routed event: %v", err)
	}
	assertQueuedEvent(t, ctx, client, first.EventID)

	unroutable := first
	unroutable.EventID = "unroutable-live-session-ended"
	unroutable.EventType = messaging.EventTypeLiveSessionEnded
	if err := publisher.Publish(ctx, unroutable); !errors.Is(err, rabbitclient.ErrPublishUnroutable) {
		t.Fatalf("publish unroutable event error = %v, want %v", err, rabbitclient.ErrPublishUnroutable)
	}
	afterReturn := integrationChatEvent("event-after-mandatory-return")
	if err := publisher.Publish(ctx, afterReturn); err != nil {
		t.Fatalf("publish after mandatory return: %v", err)
	}
	assertQueuedEvent(t, ctx, client, afterReturn.EventID)

	oldConnection, err := client.Connection(ctx)
	if err != nil {
		t.Fatalf("get current connection: %v", err)
	}
	if err := oldConnection.Close(); err != nil {
		t.Fatalf("close current connection: %v", err)
	}
	reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer reconnectCancel()
	newConnection, err := client.Connection(reconnectCtx)
	if err != nil {
		t.Fatalf("wait for reconnect: %v", err)
	}
	if newConnection == oldConnection {
		t.Fatal("RabbitMQ client did not replace the closed connection")
	}

	second := integrationChatEvent("event-after-reconnect")
	if err := publisher.Publish(reconnectCtx, second); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
	assertQueuedEvent(t, reconnectCtx, client, second.EventID)
}

func TestRealtimeSubscriberBroadcastAndReconnect(t *testing.T) {
	cfg, ok := rabbitMQIntegrationConfig(t)
	if !ok {
		t.Skip("ORION_TEST_RABBITMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := rabbitclient.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open RabbitMQ client: %v", err)
	}
	defer client.Close()
	if err := rabbitclient.InitializeCoreTopology(ctx, client, rabbitclient.DefaultPersistenceRetryDelay); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	purgePersistenceQueue(t, ctx, client)
	defer purgePersistenceQueue(t, context.Background(), client)

	hub, err := roomhub.NewHub(8)
	if err != nil {
		t.Fatalf("create Hub: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := hub.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Hub.Shutdown() error = %v", err)
		}
	}()
	roomClient, err := roomhub.NewClient(8)
	if err != nil {
		t.Fatalf("create room Client: %v", err)
	}
	if err := hub.Join(1, roomClient); err != nil {
		t.Fatalf("join room: %v", err)
	}

	subscriber, err := realtime.Start(ctx, client, hub, 8)
	if err != nil {
		t.Fatalf("start realtime subscriber: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := subscriber.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Subscriber.Shutdown() error = %v", err)
		}
	}()
	publisher, err := rabbitclient.NewPublisher(ctx, client)
	if err != nil {
		t.Fatalf("create Publisher: %v", err)
	}
	defer publisher.Close()

	beforeReconnect := integrationChatEvent("realtime-before-reconnect")
	if err := publisher.Publish(ctx, beforeReconnect); err != nil {
		t.Fatalf("publish realtime event: %v", err)
	}
	assertRealtimeEvent(t, roomClient, beforeReconnect.EventID)

	noRoom := integrationChatEvent("realtime-no-room")
	noRoom.LiveSessionID = 999
	if err := publisher.Publish(ctx, noRoom); err != nil {
		t.Fatalf("publish event without room: %v", err)
	}
	barrier := integrationChatEvent("realtime-no-room-barrier")
	if err := publisher.Publish(ctx, barrier); err != nil {
		t.Fatalf("publish barrier event: %v", err)
	}
	assertRealtimeEvent(t, roomClient, barrier.EventID)
	if hub.ClientCount(999) != 0 || hub.RoomCount() != 1 {
		t.Fatal("event without local clients created a room")
	}

	oldConnection, err := client.Connection(ctx)
	if err != nil {
		t.Fatalf("get current connection: %v", err)
	}
	if err := oldConnection.Close(); err != nil {
		t.Fatalf("close current connection: %v", err)
	}
	waitForIntegration(t, 5*time.Second, func() bool { return subscriber.Ready() != nil }, "subscriber to observe disconnect")
	waitForIntegration(t, 15*time.Second, func() bool { return subscriber.Ready() == nil }, "subscriber to recover")

	afterReconnect := integrationChatEvent("realtime-after-reconnect")
	if err := publisher.Publish(ctx, afterReconnect); err != nil {
		t.Fatalf("publish realtime event after reconnect: %v", err)
	}
	assertRealtimeEvent(t, roomClient, afterReconnect.EventID)
}

func assertQueuedEvent(t *testing.T, ctx context.Context, client *rabbitclient.Client, wantEventID string) {
	t.Helper()
	channel, err := client.Channel(ctx)
	if err != nil {
		t.Fatalf("open inspection channel: %v", err)
	}
	defer channel.Close()
	delivery, ok, err := channel.Get(rabbitclient.PersistenceQueueName, false)
	if err != nil {
		t.Fatalf("get persistence message: %v", err)
	}
	if !ok {
		t.Fatal("persistence queue is empty")
	}
	defer delivery.Ack(false)
	if delivery.MessageId != wantEventID {
		t.Fatalf("message ID = %q, want %q", delivery.MessageId, wantEventID)
	}
	if delivery.DeliveryMode != 2 {
		t.Fatalf("delivery mode = %d, want persistent mode", delivery.DeliveryMode)
	}
	var event messaging.Event
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		t.Fatalf("unmarshal queued event: %v", err)
	}
	if event.EventID != wantEventID {
		t.Fatalf("queued event ID = %q, want %q", event.EventID, wantEventID)
	}
}

func purgePersistenceQueue(t *testing.T, ctx context.Context, client *rabbitclient.Client) {
	t.Helper()
	channel, err := client.Channel(ctx)
	if err != nil {
		t.Fatalf("open purge channel: %v", err)
	}
	defer channel.Close()
	if _, err := channel.QueuePurge(rabbitclient.PersistenceQueueName, false); err != nil {
		t.Fatalf("purge persistence queue: %v", err)
	}
}

func assertRealtimeEvent(t *testing.T, client *roomhub.Client, wantEventID string) {
	t.Helper()
	select {
	case body := <-client.Outbound():
		var event messaging.Event
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatalf("unmarshal realtime event: %v", err)
		}
		if event.EventID != wantEventID {
			t.Fatalf("realtime event ID = %q, want %q", event.EventID, wantEventID)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for realtime event %q", wantEventID)
	}
}

func waitForIntegration(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func integrationChatEvent(eventID string) messaging.Event {
	return messaging.Event{
		EventID:       eventID,
		EventType:     messaging.EventTypeChatMessageAccepted,
		SchemaVersion: messaging.SchemaVersionV1,
		CorrelationID: "integration-correlation",
		UserID:        1,
		LiveSessionID: 1,
		OccurredAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`{"message_id":"message-1","content":"hello"}`),
	}
}

func rabbitMQIntegrationConfig(t *testing.T) (config.RabbitMQ, bool) {
	t.Helper()
	rawURL := os.Getenv("ORION_TEST_RABBITMQ_URL")
	if rawURL == "" {
		return config.RabbitMQ{}, false
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse ORION_TEST_RABBITMQ_URL: %v", err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("parse RabbitMQ host: %v", err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		t.Fatalf("parse RabbitMQ port: %v", err)
	}
	password, _ := parsed.User.Password()
	vhost := strings.TrimPrefix(parsed.EscapedPath(), "/")
	if vhost == "" {
		vhost = "/"
	} else {
		decoded, err := url.PathUnescape(vhost)
		if err != nil {
			t.Fatalf("decode RabbitMQ vhost: %v", err)
		}
		vhost = decoded
	}
	return config.RabbitMQ{
		Host:     host,
		Port:     port,
		User:     parsed.User.Username(),
		Password: password,
		VHost:    vhost,
	}, true
}
