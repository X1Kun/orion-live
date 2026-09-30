//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	amqp "github.com/rabbitmq/amqp091-go"
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
	if err := rabbitclient.InitializeCoreTopology(ctx, client, persistenceIntegrationConfig()); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	if err := rabbitclient.InitializeCoreTopology(ctx, client, persistenceIntegrationConfig()); err != nil {
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

func TestPersistenceQuorumDelayedRetryAndDeadLetter(t *testing.T) {
	cfg, ok := rabbitMQIntegrationConfig(t)
	if !ok {
		t.Skip("ORION_TEST_RABBITMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := rabbitclient.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open RabbitMQ client: %v", err)
	}
	defer client.Close()
	persistenceCfg := persistenceIntegrationConfig()
	if err := rabbitclient.InitializeCoreTopology(ctx, client, persistenceCfg); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	channel, err := client.Channel(ctx)
	if err != nil {
		t.Fatalf("open test channel: %v", err)
	}
	defer channel.Close()
	if _, err := channel.QueuePurge(rabbitclient.PersistenceQueueName, false); err != nil {
		t.Fatalf("purge persistence queue: %v", err)
	}
	if _, err := channel.QueuePurge(rabbitclient.PersistenceDeadLetterQueueName, false); err != nil {
		t.Fatalf("purge persistence DLQ: %v", err)
	}
	realtimeQueue, err := channel.QueueDeclare("", false, true, true, false, amqp.Table{"x-queue-type": "classic"})
	if err != nil {
		t.Fatalf("declare realtime test queue: %v", err)
	}
	if err := channel.QueueBind(realtimeQueue.Name, string(messaging.EventTypeChatMessageAccepted), rabbitclient.InteractionExchangeName, false, nil); err != nil {
		t.Fatalf("bind realtime test queue: %v", err)
	}
	realtimeDeliveries, err := channel.Consume(realtimeQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume realtime test queue: %v", err)
	}
	persistenceDeliveries, err := channel.Consume(rabbitclient.PersistenceQueueName, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume persistence queue: %v", err)
	}
	deadLetters, err := channel.Consume(rabbitclient.PersistenceDeadLetterQueueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume persistence DLQ: %v", err)
	}
	publisher, err := rabbitclient.NewPublisher(ctx, client)
	if err != nil {
		t.Fatalf("create Publisher: %v", err)
	}
	defer publisher.Close()
	event := integrationChatEvent(fmt.Sprintf("delayed-retry-%d", time.Now().UnixNano()))
	if err := publisher.Publish(ctx, event); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	select {
	case <-realtimeDeliveries:
	case <-ctx.Done():
		t.Fatal("realtime queue did not receive initial event")
	}

	deliveryTimes := make([]time.Time, 0, persistenceCfg.DeliveryLimit+1)
	for {
		select {
		case delivery := <-persistenceDeliveries:
			deliveryTimes = append(deliveryTimes, time.Now())
			if err := delivery.Reject(true); err != nil {
				t.Fatalf("reject persistence delivery: %v", err)
			}
		case deadLetter := <-deadLetters:
			if deadLetter.MessageId != event.EventID {
				t.Fatalf("DLQ event ID = %q, want %q", deadLetter.MessageId, event.EventID)
			}
			goto verifiedDeadLetter
		case <-ctx.Done():
			t.Fatalf("timed out waiting for DLQ after %d deliveries", len(deliveryTimes))
		}
	}

verifiedDeadLetter:
	if got, want := len(deliveryTimes), persistenceCfg.DeliveryLimit+1; got != want {
		t.Fatalf("persistence deliveries = %d, want %d", got, want)
	}
	for i := 1; i < len(deliveryTimes); i++ {
		minimum := persistenceCfg.RetryMinDelay * time.Duration(i)
		if minimum > persistenceCfg.RetryMaxDelay {
			minimum = persistenceCfg.RetryMaxDelay
		}
		if elapsed := deliveryTimes[i].Sub(deliveryTimes[i-1]); elapsed < minimum-75*time.Millisecond {
			t.Fatalf("retry %d delay = %s, want at least approximately %s", i, elapsed, minimum)
		}
	}
	select {
	case duplicate := <-realtimeDeliveries:
		t.Fatalf("realtime queue received retry duplicate with message ID %q", duplicate.MessageId)
	case <-time.After(persistenceCfg.RetryMaxDelay + 100*time.Millisecond):
	}
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
	if err := rabbitclient.InitializeCoreTopology(ctx, client, persistenceIntegrationConfig()); err != nil {
		t.Fatalf("initialize topology: %v", err)
	}
	purgePersistenceQueue(t, ctx, client)
	defer purgePersistenceQueue(t, context.Background(), client)

	hub, err := roomhub.NewHub(8)
	if err != nil {
		t.Fatalf("create Hub: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer reconnectCancel()
	newConnection, err := client.Connection(reconnectCtx)
	if err != nil {
		t.Fatalf("wait for RabbitMQ reconnect: %v", err)
	}
	if newConnection == oldConnection {
		t.Fatal("RabbitMQ client did not replace the closed connection")
	}
	assertRealtimeEventuallyRecovers(t, reconnectCtx, publisher, roomClient)
	if err := subscriber.Ready(); err != nil {
		t.Fatalf("Subscriber.Ready() after recovered delivery = %v", err)
	}
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

func assertRealtimeEventuallyRecovers(
	t *testing.T,
	ctx context.Context,
	publisher *rabbitclient.Publisher,
	client *roomhub.Client,
) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for realtime recovery: %v", ctx.Err())
		default:
		}

		event := integrationChatEvent(fmt.Sprintf("realtime-after-reconnect-%d", attempt))
		if err := publisher.Publish(ctx, event); err != nil {
			if errors.Is(err, rabbitclient.ErrPublishInterrupted) || errors.Is(err, rabbitclient.ErrPublishNacked) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatalf("publish realtime event after reconnect: %v", err)
		}
		select {
		case body := <-client.Outbound():
			var delivered messaging.Event
			if err := json.Unmarshal(body, &delivered); err != nil {
				t.Fatalf("unmarshal recovered realtime event: %v", err)
			}
			if delivered.EventID != event.EventID {
				t.Fatalf("recovered event ID = %q, want %q", delivered.EventID, event.EventID)
			}
			return
		case <-time.After(200 * time.Millisecond):
		}
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

func persistenceIntegrationConfig() config.Persistence {
	return config.Persistence{
		Prefetch: 8, ProcessingTimeout: time.Second,
		RetryMinDelay: 200 * time.Millisecond, RetryMaxDelay: 400 * time.Millisecond,
		DeliveryLimit: 3,
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
