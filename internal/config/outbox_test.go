package config

import (
	"testing"
	"time"
)

func TestLoadOutboxDefaults(t *testing.T) {
	for _, name := range []string{"OUTBOX_POLL_INTERVAL", "OUTBOX_BATCH_SIZE", "OUTBOX_LEASE_DURATION", "OUTBOX_MAX_ATTEMPTS", "OUTBOX_PUBLISH_TIMEOUT", "OUTBOX_RETRY_BASE_DELAY", "OUTBOX_RETRY_MAX_DELAY"} {
		t.Setenv(name, "")
	}
	cfg, err := loadOutbox()
	if err != nil {
		t.Fatalf("loadOutbox() error = %v", err)
	}
	if cfg.BatchSize != 5 || cfg.MaxAttempts != 20 || cfg.LeaseDuration != time.Minute {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestOutboxValidateLeaseCoversSequentialBatch(t *testing.T) {
	cfg := Outbox{
		PollInterval: time.Second, BatchSize: 5, LeaseDuration: 29 * time.Second,
		MaxAttempts: 3, PublishTimeout: 5 * time.Second,
		RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("lease shorter than sequential batch budget was accepted")
	}
	cfg.LeaseDuration = 30 * time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("minimum valid lease was rejected: %v", err)
	}
}
