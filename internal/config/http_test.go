package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadHTTPDefaults(t *testing.T) {
	for _, name := range []string{
		"HTTP_ADDRESS",
		"HTTP_READ_HEADER_TIMEOUT",
		"HTTP_READ_TIMEOUT",
		"HTTP_WRITE_TIMEOUT",
		"HTTP_IDLE_TIMEOUT",
		"HTTP_MAX_HEADER_BYTES",
	} {
		t.Setenv(name, "")
	}

	cfg, err := loadHTTP()
	if err != nil {
		t.Fatalf("loadHTTP() error = %v", err)
	}

	want := HTTP{
		Address:           ":8080",
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	if cfg != want {
		t.Fatalf("loadHTTP() = %#v, want %#v", cfg, want)
	}
}

func TestLoadHTTPRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		env   string
		value string
		want  string
	}{
		{name: "duration", env: "HTTP_READ_TIMEOUT", value: "later", want: "HTTP_READ_TIMEOUT must be a duration"},
		{name: "header bytes", env: "HTTP_MAX_HEADER_BYTES", value: "large", want: "HTTP_MAX_HEADER_BYTES must be an integer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.env, tt.value)
			_, err := loadHTTP()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("loadHTTP() error = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestHTTPValidate(t *testing.T) {
	valid := HTTP{
		Address:           ":8080",
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       time.Second,
		WriteTimeout:      time.Second,
		IdleTimeout:       time.Second,
		MaxHeaderBytes:    maxHTTPHeaderBytes,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid HTTP configuration rejected: %v", err)
	}

	invalidTimeout := valid
	invalidTimeout.ReadTimeout = 0
	if err := invalidTimeout.validate(); err == nil {
		t.Fatal("zero HTTP timeout was accepted")
	}

	invalidHeaderLimit := valid
	invalidHeaderLimit.MaxHeaderBytes = maxHTTPHeaderBytes + 1
	if err := invalidHeaderLimit.validate(); err == nil {
		t.Fatal("HTTP header limit above the safety bound was accepted")
	}
}
