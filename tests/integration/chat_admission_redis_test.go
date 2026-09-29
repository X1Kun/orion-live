//go:build integration

package integration_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/admission"
	"github.com/X1Kun/orion-live/internal/config"
	redisclient "github.com/go-redis/redis/v8"
)

func TestChatAdmissionIsAtomicAndBounded(t *testing.T) {
	rawURL := os.Getenv("ORION_TEST_REDIS_URL")
	if rawURL == "" {
		t.Skip("ORION_TEST_REDIS_URL is not set")
	}
	options, err := redisclient.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	redis := redisclient.NewClient(options)
	defer redis.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := redis.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis: %v", err)
	}

	cfg := config.Chat{UserRatePerSecond: 1, UserBurst: 10, RoomRatePerSecond: 1, RoomBurst: 10}
	limiter := admission.NewChatLimiter(redis, cfg)
	var allowed atomic.Int64
	var wait sync.WaitGroup
	for range 50 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			accepted, allowErr := limiter.Allow(ctx, 7, 42)
			if allowErr != nil {
				t.Errorf("Allow() error = %v", allowErr)
				return
			}
			if accepted {
				allowed.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := allowed.Load(); got != 10 {
		t.Fatalf("allowed requests = %d, want 10", got)
	}

	accepted, err := limiter.Allow(ctx, 8, 42)
	if err != nil || !accepted {
		t.Fatalf("independent room Allow() = %v, %v; want true, nil", accepted, err)
	}
}
