package websocket

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroadcastIfPresentDoesNotCreateRoom(t *testing.T) {
	hub := newTestHub(t, 1)
	present, err := hub.BroadcastIfPresent(1, []byte("message"))
	if err != nil {
		t.Fatalf("BroadcastIfPresent() error = %v", err)
	}
	if present {
		t.Fatal("BroadcastIfPresent() reported a room that does not exist")
	}
	if count := hub.RoomCount(); count != 0 {
		t.Fatalf("RoomCount() = %d, want 0", count)
	}
}

func TestHubJoinBroadcastAndLeave(t *testing.T) {
	hub := newTestHub(t, 4)
	client := newTestClient(t, 2)
	if err := hub.Join(1, client); err != nil {
		t.Fatalf("Join() error = %v", err)
	}

	message := []byte("hello")
	present, err := hub.BroadcastIfPresent(1, message)
	if err != nil || !present {
		t.Fatalf("BroadcastIfPresent() present = %v, error = %v", present, err)
	}
	message[0] = 'x'
	if got := receiveMessage(t, client); string(got) != "hello" {
		t.Fatalf("message = %q, want %q", got, "hello")
	}

	if removed := hub.Leave(1, client); !removed {
		t.Fatal("Leave() did not remove the client")
	}
	waitDone(t, client)
	if count := hub.RoomCount(); count != 0 {
		t.Fatalf("RoomCount() = %d, want 0", count)
	}
}

func TestSlowClientIsDisconnectedWithoutBlockingRoom(t *testing.T) {
	hub := newTestHub(t, 4)
	slow := newTestClient(t, 1)
	fast := newTestClient(t, 4)
	if err := hub.Join(1, slow); err != nil {
		t.Fatalf("join slow client: %v", err)
	}
	if err := hub.Join(1, fast); err != nil {
		t.Fatalf("join fast client: %v", err)
	}

	broadcast(t, hub, 1, "first")
	if got := receiveMessage(t, fast); string(got) != "first" {
		t.Fatalf("fast client first message = %q", got)
	}
	waitFor(t, func() bool { return len(slow.Outbound()) == 1 }, "slow client queue to fill")

	broadcast(t, hub, 1, "second")
	if got := receiveMessage(t, fast); string(got) != "second" {
		t.Fatalf("fast client second message = %q", got)
	}
	waitDone(t, slow)
	if count := hub.ClientCount(1); count != 1 {
		t.Fatalf("ClientCount() = %d, want 1", count)
	}
}

func TestRoomBroadcastQueueIsBounded(t *testing.T) {
	r := &room{
		broadcastCh: make(chan []byte, 1),
		stopCh:      make(chan struct{}),
	}
	if err := r.enqueue([]byte("first")); err != nil {
		t.Fatalf("first enqueue error = %v", err)
	}
	if err := r.enqueue([]byte("second")); !errors.Is(err, ErrRoomQueueFull) {
		t.Fatalf("second enqueue error = %v, want %v", err, ErrRoomQueueFull)
	}
}

func TestHubShutdownClosesClientsAndRejectsNewWork(t *testing.T) {
	hub := newTestHub(t, 2)
	first := newTestClient(t, 1)
	second := newTestClient(t, 1)
	if err := hub.Join(1, first); err != nil {
		t.Fatalf("join first room: %v", err)
	}
	if err := hub.Join(2, second); err != nil {
		t.Fatalf("join second room: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	waitDone(t, first)
	waitDone(t, second)

	if err := hub.Join(3, newTestClient(t, 1)); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("Join() after shutdown error = %v, want %v", err, ErrHubClosed)
	}
	if _, err := hub.BroadcastIfPresent(1, []byte("message")); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("BroadcastIfPresent() after shutdown error = %v, want %v", err, ErrHubClosed)
	}
}

func TestHubConcurrentJoinBroadcastAndLeave(t *testing.T) {
	hub := newTestHub(t, 128)
	const workers = 64
	clients := make([]*Client, workers)
	for index := range clients {
		clients[index] = newTestClient(t, workers)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for index := 0; index < workers; index++ {
		go func(index int) {
			defer waitGroup.Done()
			client := clients[index]
			if err := hub.Join(1, client); err != nil {
				t.Errorf("worker %d Join() error = %v", index, err)
				return
			}
			present, err := hub.BroadcastIfPresent(1, []byte(fmt.Sprintf("message-%d", index)))
			if err != nil && !errors.Is(err, ErrRoomQueueFull) {
				t.Errorf("worker %d BroadcastIfPresent() error = %v", index, err)
			}
			if !present {
				t.Errorf("worker %d did not observe a room", index)
			}
			hub.Leave(1, client)
		}(index)
	}
	waitGroup.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestHubRejectsInvalidConstructionAndJoin(t *testing.T) {
	if _, err := NewHub(0); !errors.Is(err, ErrInvalidRoomQueueCapacity) {
		t.Fatalf("NewHub(0) error = %v", err)
	}
	if _, err := NewClient(0); !errors.Is(err, ErrInvalidClientQueueCapacity) {
		t.Fatalf("NewClient(0) error = %v", err)
	}

	hub := newTestHub(t, 1)
	if err := hub.Join(0, newTestClient(t, 1)); !errors.Is(err, ErrInvalidLiveSessionID) {
		t.Fatalf("Join(0) error = %v", err)
	}
	if err := hub.Join(1, nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Join(nil) error = %v", err)
	}
}

func TestClientCanJoinOnlyOneRoom(t *testing.T) {
	hub := newTestHub(t, 1)
	client := newTestClient(t, 1)
	if err := hub.Join(1, client); err != nil {
		t.Fatalf("first Join() error = %v", err)
	}
	if err := hub.Join(1, client); !errors.Is(err, ErrClientAlreadyJoined) {
		t.Fatalf("duplicate Join() error = %v, want %v", err, ErrClientAlreadyJoined)
	}
	if err := hub.Join(2, client); !errors.Is(err, ErrClientAlreadyJoined) {
		t.Fatalf("cross-room Join() error = %v, want %v", err, ErrClientAlreadyJoined)
	}
	if count := hub.RoomCount(); count != 1 {
		t.Fatalf("RoomCount() = %d, want 1", count)
	}

	hub.Leave(1, client)
	if err := hub.Join(2, client); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Join() after Leave() error = %v, want %v", err, ErrClientClosed)
	}
}

func TestConcurrentJoinsClaimClientOnce(t *testing.T) {
	hub := newTestHub(t, 1)
	client := newTestClient(t, 1)
	const attempts = 32

	results := make(chan error, attempts)
	var waitGroup sync.WaitGroup
	waitGroup.Add(attempts)
	for index := 1; index <= attempts; index++ {
		go func(liveSessionID uint64) {
			defer waitGroup.Done()
			results <- hub.Join(liveSessionID, client)
		}(uint64(index))
	}
	waitGroup.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrClientAlreadyJoined):
		default:
			t.Fatalf("Join() error = %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful joins = %d, want 1", succeeded)
	}
	if count := hub.RoomCount(); count != 1 {
		t.Fatalf("RoomCount() = %d, want 1", count)
	}
}

func newTestHub(t *testing.T, capacity int) *Hub {
	t.Helper()
	hub, err := NewHub(capacity)
	if err != nil {
		t.Fatalf("NewHub() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := hub.Shutdown(ctx); err != nil {
			t.Errorf("cleanup Shutdown() error = %v", err)
		}
	})
	return hub
}

func newTestClient(t *testing.T, capacity int) *Client {
	t.Helper()
	client, err := NewClient(capacity)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func broadcast(t *testing.T, hub *Hub, liveSessionID uint64, message string) {
	t.Helper()
	present, err := hub.BroadcastIfPresent(liveSessionID, []byte(message))
	if err != nil || !present {
		t.Fatalf("BroadcastIfPresent() present = %v, error = %v", present, err)
	}
}

func receiveMessage(t *testing.T, client *Client) []byte {
	t.Helper()
	select {
	case message := <-client.Outbound():
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func waitDone(t *testing.T, client *Client) {
	t.Helper()
	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client to close")
	}
}

func waitFor(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}
