package config

import (
	"errors"
	"net"
	"net/url"
	"os"
)

type RabbitMQ struct {
	Host             string
	Port             string
	User             string
	Password         string
	VHost            string
	RealtimePrefetch int
}

func loadRabbitMQ() (RabbitMQ, error) {
	realtimePrefetch, err := envInt("RABBITMQ_REALTIME_PREFETCH", 64)
	if err != nil {
		return RabbitMQ{}, err
	}
	return RabbitMQ{
		Host:             env("RABBITMQ_HOST", "127.0.0.1"),
		Port:             env("RABBITMQ_PORT", "5672"),
		User:             os.Getenv("RABBITMQ_USER"),
		Password:         os.Getenv("RABBITMQ_PASSWORD"),
		VHost:            env("RABBITMQ_VHOST", "/"),
		RealtimePrefetch: realtimePrefetch,
	}, nil
}

func (c RabbitMQ) validate() error {
	if c.RealtimePrefetch <= 0 {
		return errors.New("RABBITMQ_REALTIME_PREFETCH must be positive")
	}
	return nil
}

func (c RabbitMQ) URL() string {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.User, c.Password),
		Host:   net.JoinHostPort(c.Host, c.Port),
		Path:   c.VHost,
	}
	return u.String()
}
