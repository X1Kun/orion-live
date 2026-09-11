package websocket

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrHubClosed                = errors.New("websocket hub is closed")
	ErrInvalidRoomQueueCapacity = errors.New("room broadcast queue capacity must be positive")
	ErrInvalidLiveSessionID     = errors.New("live session id must be positive")
	ErrNilClient                = errors.New("websocket client must not be nil")
)

// Hub owns the process-local mapping between live sessions and rooms.
type Hub struct {
	mu                         sync.RWMutex
	rooms                      map[uint64]*room
	roomBroadcastQueueCapacity int
	closed                     bool
	roomsWG                    sync.WaitGroup
}

func NewHub(roomBroadcastQueueCapacity int) (*Hub, error) {
	if roomBroadcastQueueCapacity <= 0 {
		return nil, ErrInvalidRoomQueueCapacity
	}
	return &Hub{
		rooms:                      make(map[uint64]*room),
		roomBroadcastQueueCapacity: roomBroadcastQueueCapacity,
	}, nil
}

// Join registers a client with a process-local room, creating the room when needed.
func (h *Hub) Join(liveSessionID uint64, client *Client) error {
	if liveSessionID == 0 {
		return ErrInvalidLiveSessionID
	}
	if client == nil {
		return ErrNilClient
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHubClosed
	}
	if err := client.claimRoom(); err != nil {
		return err
	}
	if existing := h.rooms[liveSessionID]; existing != nil {
		if err := existing.add(client); err != nil {
			client.rollbackRoomClaim()
			return err
		}
		return nil
	}

	created := newRoom(liveSessionID, h, h.roomBroadcastQueueCapacity)
	if err := created.add(client); err != nil {
		client.rollbackRoomClaim()
		return err
	}
	h.rooms[liveSessionID] = created
	h.roomsWG.Add(1)
	go func() {
		defer h.roomsWG.Done()
		created.run()
	}()
	return nil
}

// Leave unregisters and closes a client. It removes the room when the last client leaves.
func (h *Hub) Leave(liveSessionID uint64, client *Client) bool {
	if liveSessionID == 0 || client == nil {
		return false
	}

	h.mu.Lock()
	current := h.rooms[liveSessionID]
	if current == nil {
		h.mu.Unlock()
		return false
	}
	removed, empty := current.remove(client)
	if empty {
		delete(h.rooms, liveSessionID)
	}
	h.mu.Unlock()

	if empty {
		current.requestStop()
	}
	return removed
}

// BroadcastIfPresent queues a message only when the process already owns the room.
func (h *Hub) BroadcastIfPresent(liveSessionID uint64, message []byte) (bool, error) {
	if liveSessionID == 0 {
		return false, ErrInvalidLiveSessionID
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return false, ErrHubClosed
	}
	current := h.rooms[liveSessionID]
	if current == nil {
		return false, nil
	}
	return true, current.enqueue(message)
}

// Shutdown rejects new work, closes every client, and waits for all rooms to stop.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
	}
	rooms := make([]*room, 0, len(h.rooms))
	for id, current := range h.rooms {
		rooms = append(rooms, current)
		delete(h.rooms, id)
	}
	h.mu.Unlock()

	for _, current := range rooms {
		current.requestStop()
	}
	roomsStopped := make(chan struct{})
	go func() {
		h.roomsWG.Wait()
		close(roomsStopped)
	}()
	select {
	case <-roomsStopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RoomCount returns the number of process-local rooms.
func (h *Hub) RoomCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms)
}

// ClientCount returns the number of clients in a process-local room.
func (h *Hub) ClientCount(liveSessionID uint64) int {
	h.mu.RLock()
	current := h.rooms[liveSessionID]
	if current == nil {
		h.mu.RUnlock()
		return 0
	}
	count := current.clientCount()
	h.mu.RUnlock()
	return count
}

func (h *Hub) removeIfEmpty(candidate *room) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.rooms[candidate.id]
	if current != candidate || !candidate.isEmpty() {
		return false
	}
	delete(h.rooms, candidate.id)
	return true
}
