package loadtest

import (
	"errors"
	"testing"
	"time"
)

func TestHoldConnectionsCompletesWithoutReaderError(t *testing.T) {
	if err := holdConnections(t.Context(), time.Millisecond, newCollector(1)); err != nil {
		t.Fatalf("holdConnections() error = %v", err)
	}
}

func TestHoldConnectionsReturnsReaderError(t *testing.T) {
	want := errors.New("connection closed")
	results := newCollector(1)
	results.setError(want)
	if err := holdConnections(t.Context(), time.Second, results); !errors.Is(err, want) {
		t.Fatalf("holdConnections() error = %v, want %v", err, want)
	}
}
