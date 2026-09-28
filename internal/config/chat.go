package config

import (
	"errors"
	"time"
)

type Chat struct {
	PublishTimeout time.Duration
}

func loadChat() (Chat, error) {
	publishTimeout, err := envDuration("CHAT_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return Chat{}, err
	}
	return Chat{PublishTimeout: publishTimeout}, nil
}

func (c Chat) validate() error {
	if c.PublishTimeout <= 0 {
		return errors.New("CHAT_PUBLISH_TIMEOUT must be positive")
	}
	return nil
}
