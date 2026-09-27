package config

import (
	"errors"
	"fmt"
	"time"
)

type Outbox struct {
	PollInterval   time.Duration
	BatchSize      int
	LeaseDuration  time.Duration
	MaxAttempts    int
	PublishTimeout time.Duration
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

func loadOutbox() (Outbox, error) {
	pollInterval, err := envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return Outbox{}, err
	}
	batchSize, err := envInt("OUTBOX_BATCH_SIZE", 5)
	if err != nil {
		return Outbox{}, err
	}
	leaseDuration, err := envDuration("OUTBOX_LEASE_DURATION", time.Minute)
	if err != nil {
		return Outbox{}, err
	}
	maxAttempts, err := envInt("OUTBOX_MAX_ATTEMPTS", 20)
	if err != nil {
		return Outbox{}, err
	}
	publishTimeout, err := envDuration("OUTBOX_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return Outbox{}, err
	}
	retryBaseDelay, err := envDuration("OUTBOX_RETRY_BASE_DELAY", time.Second)
	if err != nil {
		return Outbox{}, err
	}
	retryMaxDelay, err := envDuration("OUTBOX_RETRY_MAX_DELAY", time.Minute)
	if err != nil {
		return Outbox{}, err
	}
	return Outbox{
		PollInterval:   pollInterval,
		BatchSize:      batchSize,
		LeaseDuration:  leaseDuration,
		MaxAttempts:    maxAttempts,
		PublishTimeout: publishTimeout,
		RetryBaseDelay: retryBaseDelay,
		RetryMaxDelay:  retryMaxDelay,
	}, nil
}

func (c Outbox) validate() error {
	if c.PollInterval <= 0 || c.LeaseDuration <= 0 || c.PublishTimeout <= 0 || c.RetryBaseDelay <= 0 || c.RetryMaxDelay <= 0 {
		return errors.New("Outbox durations must be positive")
	}
	if c.BatchSize <= 0 || c.MaxAttempts <= 0 {
		return errors.New("OUTBOX_BATCH_SIZE and OUTBOX_MAX_ATTEMPTS must be positive")
	}
	if c.RetryBaseDelay > c.RetryMaxDelay {
		return errors.New("OUTBOX_RETRY_BASE_DELAY must not exceed OUTBOX_RETRY_MAX_DELAY")
	}
	minimumLease := time.Duration(c.BatchSize+1) * c.PublishTimeout
	if c.LeaseDuration < minimumLease {
		return fmt.Errorf(
			"OUTBOX_LEASE_DURATION must be at least %s for batch size %d and publish timeout %s",
			minimumLease,
			c.BatchSize,
			c.PublishTimeout,
		)
	}
	return nil
}
