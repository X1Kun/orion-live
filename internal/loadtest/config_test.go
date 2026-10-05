package loadtest

import (
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() error = %v", err)
	}
	if cfg.TargetMessages() != 10 {
		t.Fatalf("TargetMessages() = %d, want 10", cfg.TargetMessages())
	}
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "mode", mutate: func(c *Config) { c.Mode = "unknown" }},
		{name: "relative URL", mutate: func(c *Config) { c.BaseURL = "/api" }},
		{name: "connections", mutate: func(c *Config) { c.Connections = 0 }},
		{name: "connections per user", mutate: func(c *Config) { c.ConnectionsPerUser = c.Connections + 1 }},
		{name: "message rate", mutate: func(c *Config) { c.MessageRate = 0 }},
		{name: "duration", mutate: func(c *Config) { c.Duration = 0 }},
		{name: "request timeout", mutate: func(c *Config) { c.RequestTimeout = 0 }},
		{name: "drain timeout", mutate: func(c *Config) { c.DrainTimeout = 0 }},
		{name: "history timeout", mutate: func(c *Config) { c.HistoryTimeout = 0 }},
		{name: "error rate", mutate: func(c *Config) { c.MaxErrorRate = 1.1 }},
		{name: "no target messages", mutate: func(c *Config) { c.Duration = time.Nanosecond }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestConnectionsModeAllowsNoMessages(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = ModeConnections
	cfg.MessageRate = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("connections config rejected: %v", err)
	}
	if cfg.TargetMessages() != 0 {
		t.Fatalf("TargetMessages() = %d, want 0", cfg.TargetMessages())
	}
}

func TestConnectionURLs(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.ConnectionURLs(); len(got) != 1 || got[0] != cfg.BaseURL {
		t.Fatalf("default ConnectionURLs() = %v", got)
	}
	cfg.ConnectionBaseURLs = []string{"http://api-a", "http://api-b"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("multi-endpoint config rejected: %v", err)
	}
}
