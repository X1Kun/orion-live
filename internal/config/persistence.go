package config

import (
	"errors"
	"time"
)

type Persistence struct {
	Prefetch          int
	Concurrency       int
	ProcessingTimeout time.Duration
	RetryMinDelay     time.Duration
	RetryMaxDelay     time.Duration
	DeliveryLimit     int
}

func loadPersistence() (Persistence, error) {
	prefetch, err := envInt("PERSISTENCE_PREFETCH", 32)
	if err != nil {
		return Persistence{}, err
	}
	concurrency, err := envInt("PERSISTENCE_CONCURRENCY", 4)
	if err != nil {
		return Persistence{}, err
	}
	processingTimeout, err := envDuration("PERSISTENCE_PROCESSING_TIMEOUT", 3*time.Second)
	if err != nil {
		return Persistence{}, err
	}
	retryMinDelay, err := envDuration("PERSISTENCE_RETRY_MIN_DELAY", 5*time.Second)
	if err != nil {
		return Persistence{}, err
	}
	retryMaxDelay, err := envDuration("PERSISTENCE_RETRY_MAX_DELAY", 30*time.Second)
	if err != nil {
		return Persistence{}, err
	}
	deliveryLimit, err := envInt("PERSISTENCE_DELIVERY_LIMIT", 5)
	if err != nil {
		return Persistence{}, err
	}
	return Persistence{
		Prefetch: prefetch, Concurrency: concurrency, ProcessingTimeout: processingTimeout,
		RetryMinDelay: retryMinDelay, RetryMaxDelay: retryMaxDelay,
		DeliveryLimit: deliveryLimit,
	}, nil
}

func (c Persistence) validate() error {
	if c.Prefetch <= 0 {
		return errors.New("PERSISTENCE_PREFETCH must be positive")
	}
	if c.Concurrency <= 0 || c.Concurrency > 64 {
		return errors.New("PERSISTENCE_CONCURRENCY must be between 1 and 64")
	}
	if c.Concurrency > c.Prefetch {
		return errors.New("PERSISTENCE_CONCURRENCY must not exceed PERSISTENCE_PREFETCH")
	}
	if c.ProcessingTimeout <= 0 {
		return errors.New("PERSISTENCE_PROCESSING_TIMEOUT must be positive")
	}
	if c.RetryMinDelay < time.Millisecond || c.RetryMaxDelay < time.Millisecond {
		return errors.New("Persistence retry delays must be at least 1ms")
	}
	if c.RetryMinDelay > c.RetryMaxDelay {
		return errors.New("PERSISTENCE_RETRY_MIN_DELAY must not exceed PERSISTENCE_RETRY_MAX_DELAY")
	}
	if c.DeliveryLimit <= 0 {
		return errors.New("PERSISTENCE_DELIVERY_LIMIT must be positive")
	}
	return nil
}
