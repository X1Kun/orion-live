package config

import (
	"testing"
	"time"
)

func TestLoadPersistenceDefaults(t *testing.T) {
	for _, name := range []string{"PERSISTENCE_PREFETCH", "PERSISTENCE_PROCESSING_TIMEOUT", "PERSISTENCE_RETRY_MIN_DELAY", "PERSISTENCE_RETRY_MAX_DELAY", "PERSISTENCE_DELIVERY_LIMIT"} {
		t.Setenv(name, "")
	}
	cfg, err := loadPersistence()
	if err != nil {
		t.Fatalf("loadPersistence() error = %v", err)
	}
	if cfg.Prefetch != 32 || cfg.ProcessingTimeout != 3*time.Second || cfg.RetryMinDelay != 5*time.Second || cfg.RetryMaxDelay != 30*time.Second || cfg.DeliveryLimit != 5 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}
