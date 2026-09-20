package config

import (
	"errors"
	"time"
)

const maxHTTPHeaderBytes = 1 << 20

type HTTP struct {
	Address           string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

func loadHTTP() (HTTP, error) {
	readHeaderTimeout, err := envDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second)
	if err != nil {
		return HTTP{}, err
	}
	readTimeout, err := envDuration("HTTP_READ_TIMEOUT", 10*time.Second)
	if err != nil {
		return HTTP{}, err
	}
	writeTimeout, err := envDuration("HTTP_WRITE_TIMEOUT", 15*time.Second)
	if err != nil {
		return HTTP{}, err
	}
	idleTimeout, err := envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return HTTP{}, err
	}
	maxHeaderBytes, err := envInt("HTTP_MAX_HEADER_BYTES", maxHTTPHeaderBytes)
	if err != nil {
		return HTTP{}, err
	}

	return HTTP{
		Address:           env("HTTP_ADDRESS", ":8080"),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}, nil
}

func (c HTTP) validate() error {
	if c.ReadHeaderTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.IdleTimeout <= 0 {
		return errors.New("HTTP timeouts must be positive")
	}
	if c.MaxHeaderBytes <= 0 || c.MaxHeaderBytes > maxHTTPHeaderBytes {
		return errors.New("HTTP_MAX_HEADER_BYTES must be between 1 and 1048576")
	}
	return nil
}
