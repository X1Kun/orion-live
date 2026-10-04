package config

import "testing"

func TestLoadRabbitMQDefaults(t *testing.T) {
	t.Setenv("RABBITMQ_REALTIME_PREFETCH", "")
	t.Setenv("RABBITMQ_CHAT_PUBLISH_CONCURRENCY", "")
	cfg, err := loadRabbitMQ()
	if err != nil {
		t.Fatalf("loadRabbitMQ() error = %v", err)
	}
	if cfg.RealtimePrefetch != 64 {
		t.Fatalf("RealtimePrefetch = %d, want 64", cfg.RealtimePrefetch)
	}
	if cfg.ChatPublishConcurrency != 8 {
		t.Fatalf("ChatPublishConcurrency = %d, want 8", cfg.ChatPublishConcurrency)
	}
}

func TestRabbitMQValidate(t *testing.T) {
	if err := (RabbitMQ{RealtimePrefetch: 1, ChatPublishConcurrency: 1}).validate(); err != nil {
		t.Fatalf("valid RabbitMQ configuration rejected: %v", err)
	}
	for _, cfg := range []RabbitMQ{
		{ChatPublishConcurrency: 1},
		{RealtimePrefetch: 1},
		{RealtimePrefetch: 1, ChatPublishConcurrency: 65},
	} {
		if err := cfg.validate(); err == nil {
			t.Fatalf("invalid RabbitMQ configuration was accepted: %#v", cfg)
		}
	}
}
