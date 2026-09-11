package websocket

import (
	"errors"
	"sync"
)

var ErrRoomQueueFull = errors.New("room broadcast queue is full")

type room struct {
	id          uint64
	hub         *Hub
	broadcastCh chan []byte
	stopCh      chan struct{}
	stopOnce    sync.Once

	mu      sync.RWMutex
	clients map[*Client]struct{}
	closed  bool
}

func newRoom(id uint64, hub *Hub, broadcastQueueCapacity int) *room {
	return &room{
		id:          id,
		hub:         hub,
		broadcastCh: make(chan []byte, broadcastQueueCapacity),
		stopCh:      make(chan struct{}),
		clients:     make(map[*Client]struct{}),
	}
}

func (r *room) run() {
	for {
		select {
		case message := <-r.broadcastCh:
			if r.deliver(message) && r.hub.removeIfEmpty(r) {
				r.markClosed()
				return
			}
		case <-r.stopCh:
			r.closeAll()
			return
		}
	}
}

func (r *room) add(client *Client) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrHubClosed
	}
	if client.isClosed() {
		return ErrClientClosed
	}
	r.clients[client] = struct{}{}
	return nil
}

func (r *room) remove(client *Client) (removed, empty bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.clients[client]; !exists {
		return false, len(r.clients) == 0
	}
	delete(r.clients, client)
	client.close()
	return true, len(r.clients) == 0
}

func (r *room) enqueue(message []byte) error {
	copyOfMessage := append([]byte(nil), message...)
	select {
	case <-r.stopCh:
		return ErrHubClosed
	default:
	}
	select {
	case r.broadcastCh <- copyOfMessage:
		return nil
	case <-r.stopCh:
		return ErrHubClosed
	default:
		return ErrRoomQueueFull
	}
}

func (r *room) deliver(message []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for client := range r.clients {
		if client.enqueueOutbound(message) {
			continue
		}
		delete(r.clients, client)
		client.close()
	}
	return len(r.clients) == 0
}

func (r *room) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients) == 0
}

func (r *room) clientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

func (r *room) requestStop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

func (r *room) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for client := range r.clients {
		delete(r.clients, client)
		client.close()
	}
}

func (r *room) markClosed() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}
