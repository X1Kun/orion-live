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
	rabbitclient "github.com/X1Kun/orion-live/pkg/rabbitmq"
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

func assertQueuedEvent(t *testing.T, ctx context.Context, client *rabbitclient.Client, wantEventID string) {
	t.Helper()
	channel, err := client.Channel(ctx)
	if err != nil {
		t.Fatalf("open inspection channel: %v", err)
	}
	defer channel.Close()
	delivery, ok, err := channel.Get(messaging.PersistenceQueueName, false)
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
