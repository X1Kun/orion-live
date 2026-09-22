package config

import "testing"

func TestLoadRabbitMQDefaults(t *testing.T) {
	t.Setenv("RABBITMQ_REALTIME_PREFETCH", "")
	cfg, err := loadRabbitMQ()
	if err != nil {
		t.Fatalf("loadRabbitMQ() error = %v", err)
	}
	if cfg.RealtimePrefetch != 64 {
		t.Fatalf("RealtimePrefetch = %d, want 64", cfg.RealtimePrefetch)
	}
}

func TestRabbitMQValidate(t *testing.T) {
	if err := (RabbitMQ{RealtimePrefetch: 1}).validate(); err != nil {
		t.Fatalf("valid RabbitMQ configuration rejected: %v", err)
	}
	if err := (RabbitMQ{}).validate(); err == nil {
		t.Fatal("non-positive realtime prefetch was accepted")
	}
}
