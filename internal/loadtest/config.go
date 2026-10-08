package loadtest

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"
)

type Mode string

const (
	ModeChat        Mode = "chat"
	ModeConnections Mode = "connections"
)

type Config struct {
	Mode               Mode
	BaseURL            string
	ConnectionBaseURLs []string
	Connections        int
	ConnectionsPerUser int
	MessageRate        int
	Senders            int
	BurstRate          int
	BurstDuration      time.Duration
	Duration           time.Duration
	RequestTimeout     time.Duration
	DrainTimeout       time.Duration
	HistoryTimeout     time.Duration
	MaxErrorRate       float64
}

func DefaultConfig() Config {
	return Config{
		Mode:               ModeChat,
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
	if c.Senders < 0 || c.Senders > c.Connections {
		return errors.New("senders must be between zero and connections (zero uses all connections)")
	}
	if c.BurstRate < 0 || c.BurstDuration < 0 || (c.BurstRate == 0) != (c.BurstDuration == 0) {
		return errors.New("burst rate and duration must both be positive or both zero")
	}
	if c.BurstRate > int(time.Second) {
		return errors.New("burst rate is too high")
	}
	if c.Mode != ModeChat && c.Mode != ModeConnections {
		return errors.New("mode must be chat or connections")
	}
	parsed, err := url.ParseRequestURI(c.BaseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("base URL must be an absolute HTTP(S) URL")
	}
	for _, baseURL := range c.ConnectionURLs() {
		parsed, err := url.ParseRequestURI(baseURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("connection base URLs must be absolute HTTP(S) URLs")
		}
	}
	if c.Connections <= 0 {
		return errors.New("connections must be positive")
	}
	if c.ConnectionsPerUser <= 0 || c.ConnectionsPerUser > c.Connections {
		return errors.New("connections per user must be between 1 and the connection count")
	}
	if c.Mode == ModeChat && (c.MessageRate <= 0 || time.Second/time.Duration(c.MessageRate) <= 0) {
		return errors.New("message rate must be positive and representable in chat mode")
	}
	if c.Duration <= 0 || c.RequestTimeout <= 0 || c.DrainTimeout <= 0 || c.HistoryTimeout <= 0 {
		return errors.New("timeouts and duration must be positive")
	}
	if c.MaxErrorRate < 0 || c.MaxErrorRate > 1 {
		return errors.New("max error rate must be between 0 and 1")
	}
	if c.Mode == ModeChat && c.TargetMessages() <= 0 {
		return fmt.Errorf("duration and message rate must produce at least one message")
	}
	return nil
}

func (c Config) ConnectionURLs() []string {
	if len(c.ConnectionBaseURLs) == 0 {
		return []string{c.BaseURL}
	}
	return slices.Clone(c.ConnectionBaseURLs)
}

func (c Config) TargetMessages() int {
	if c.Mode == ModeConnections {
		return 0
	}
	return int(c.Duration*time.Duration(c.MessageRate)/time.Second) + int(c.BurstDuration*time.Duration(c.BurstRate)/time.Second)
}
