package loadtest

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

type Config struct {
	BaseURL            string
	Connections        int
	ConnectionsPerUser int
	MessageRate        int
	Duration           time.Duration
	RequestTimeout     time.Duration
	DrainTimeout       time.Duration
	HistoryTimeout     time.Duration
	MaxErrorRate       float64
}

func DefaultConfig() Config {
	return Config{
		BaseURL:            "http://127.0.0.1:8080",
		Connections:        10,
		ConnectionsPerUser: 1,
		MessageRate:        1,
		Duration:           10 * time.Second,
		RequestTimeout:     10 * time.Second,
		DrainTimeout:       15 * time.Second,
		HistoryTimeout:     20 * time.Second,
	}
}

func (c Config) Validate() error {
	parsed, err := url.ParseRequestURI(c.BaseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("base URL must be an absolute HTTP(S) URL")
	}
	if c.Connections <= 0 {
		return errors.New("connections must be positive")
	}
	if c.ConnectionsPerUser <= 0 || c.ConnectionsPerUser > c.Connections {
		return errors.New("connections per user must be between 1 and the connection count")
	}
	if c.MessageRate <= 0 || time.Second/time.Duration(c.MessageRate) <= 0 {
		return errors.New("message rate must be positive and representable")
	}
	if c.Duration <= 0 || c.RequestTimeout <= 0 || c.DrainTimeout <= 0 || c.HistoryTimeout <= 0 {
		return errors.New("timeouts and duration must be positive")
	}
	if c.MaxErrorRate < 0 || c.MaxErrorRate > 1 {
		return errors.New("max error rate must be between 0 and 1")
	}
	if c.TargetMessages() <= 0 {
		return fmt.Errorf("duration and message rate must produce at least one message")
	}
	return nil
}

func (c Config) TargetMessages() int {
	return int(c.Duration * time.Duration(c.MessageRate) / time.Second)
}
