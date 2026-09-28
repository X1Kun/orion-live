package config

import (
	"testing"
	"time"
)

func TestLoadChatDefaults(t *testing.T) {
	t.Setenv("CHAT_PUBLISH_TIMEOUT", "")
	cfg, err := loadChat()
	if err != nil {
		t.Fatalf("loadChat() error = %v", err)
	}
	if cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("PublishTimeout = %s, want 5s", cfg.PublishTimeout)
	}
}

func TestChatValidate(t *testing.T) {
	if err := (Chat{}).validate(); err == nil {
		t.Fatal("zero publish timeout was accepted")
	}
	if err := (Chat{PublishTimeout: time.Second}).validate(); err != nil {
		t.Fatalf("valid Chat config was rejected: %v", err)
	}
}
