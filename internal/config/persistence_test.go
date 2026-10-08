package config

import (
	"testing"
	"time"
)

func TestLoadPersistenceDefaults(t *testing.T) {
	for _, name := range []string{"PERSISTENCE_PREFETCH", "PERSISTENCE_CONCURRENCY", "PERSISTENCE_PROCESSING_TIMEOUT", "PERSISTENCE_RETRY_MIN_DELAY", "PERSISTENCE_RETRY_MAX_DELAY", "PERSISTENCE_DELIVERY_LIMIT"} {
		t.Setenv(name, "")
	}
	cfg, err := loadPersistence()
	if err != nil {
		t.Fatalf("loadPersistence() error = %v", err)
	}
	if cfg.Prefetch != 32 || cfg.Concurrency != 4 || cfg.ProcessingTimeout != 3*time.Second || cfg.RetryMinDelay != 5*time.Second || cfg.RetryMaxDelay != 30*time.Second || cfg.DeliveryLimit != 5 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestPersistenceValidateConcurrency(t *testing.T) {
	valid := Persistence{
		Prefetch: 32, Concurrency: 4, ProcessingTimeout: time.Second,
		RetryMinDelay: time.Second, RetryMaxDelay: time.Second, DeliveryLimit: 1,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid Persistence configuration rejected: %v", err)
	}
	for _, concurrency := range []int{0, 33, 65} {
		cfg := valid
		cfg.Concurrency = concurrency
		if err := cfg.validate(); err == nil {
			t.Fatalf("Concurrency %d was accepted", concurrency)
		}
	}
}
