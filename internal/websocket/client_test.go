package websocket

import (
	"testing"
	"time"
)

func TestOutboundMessageHasTimestampAndOwnsBody(t *testing.T) {
	client, err := NewClient(1)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("hello")
	before := time.Now()
	if !client.EnqueueOutbound(body) {
		t.Fatal("enqueue failed")
	}
	body[0] = 'x'
	message := <-client.Outbound()
	if string(message.Body) != "hello" {
		t.Fatalf("body=%s", message.Body)
	}
	if message.EnqueuedAt.Before(before) || message.EnqueuedAt.After(time.Now()) {
		t.Fatal("invalid enqueue timestamp")
	}
}
