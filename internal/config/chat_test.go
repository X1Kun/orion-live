package config

import (
	"testing"
	"time"
)

func TestLoadChatDefaults(t *testing.T) {
	for _, name := range []string{
		"CHAT_PUBLISH_TIMEOUT", "CHAT_ADMISSION_TIMEOUT", "CHAT_USER_RATE_PER_SECOND",
		"CHAT_USER_BURST", "CHAT_ROOM_RATE_PER_SECOND", "CHAT_ROOM_BURST",
	} {
		t.Setenv(name, "")
	}
	cfg, err := loadChat()
	if err != nil {
		t.Fatalf("loadChat() error = %v", err)
	}
	if cfg.PublishTimeout != 5*time.Second || cfg.AdmissionTimeout != 100*time.Millisecond || cfg.UserRatePerSecond != 5 || cfg.UserBurst != 10 || cfg.RoomRatePerSecond != 100 || cfg.RoomBurst != 200 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestChatValidate(t *testing.T) {
	if err := (Chat{}).validate(); err == nil {
		t.Fatal("zero publish timeout was accepted")
	}
	if err := (Chat{
		PublishTimeout: time.Second, AdmissionTimeout: time.Millisecond,
		UserRatePerSecond: 1, UserBurst: 1, RoomRatePerSecond: 1, RoomBurst: 1,
	}).validate(); err != nil {
		t.Fatalf("valid Chat config was rejected: %v", err)
	}
}
