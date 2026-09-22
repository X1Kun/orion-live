package config

import (
	"errors"
	"os"
	"time"
)

type Config struct {
	Environment            string
	DependencyInitTimeout  time.Duration
	ProcessShutdownTimeout time.Duration
	JWTSecret              string
	AccessTokenTTL         time.Duration
	HTTP                   HTTP
	WebSocket              WebSocket
	MySQL                  MySQL
	Redis                  Redis
	RabbitMQ               RabbitMQ
}

func Load() (Config, error) {
	httpConfig, err := loadHTTP()
	if err != nil {
		return Config{}, err
	}
	webSocketConfig, err := loadWebSocket()
	if err != nil {
		return Config{}, err
	}
	mysqlConfig, err := loadMySQL()
	if err != nil {
		return Config{}, err
	}
	redisConfig, err := loadRedis()
	if err != nil {
		return Config{}, err
	}
	rabbitMQConfig, err := loadRabbitMQ()
	if err != nil {
		return Config{}, err
	}
	dependencyInitTimeout, err := envDuration("DEPENDENCY_INIT_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	processShutdownTimeout, err := envDuration("PROCESS_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	accessTokenTTL, err := envDuration("ACCESS_TOKEN_TTL", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Environment:            env("APP_ENV", "development"),
		DependencyInitTimeout:  dependencyInitTimeout,
		ProcessShutdownTimeout: processShutdownTimeout,
		JWTSecret:              os.Getenv("JWT_SECRET_KEY"),
		AccessTokenTTL:         accessTokenTTL,
		HTTP:                   httpConfig,
		WebSocket:              webSocketConfig,
		MySQL:                  mysqlConfig,
		Redis:                  redisConfig,
		RabbitMQ:               rabbitMQConfig,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if err := validateRequired(map[string]string{
		"DB_USER":           c.MySQL.User,
		"DB_PASSWORD":       c.MySQL.Password,
		"DB_NAME":           c.MySQL.Database,
		"HTTP_ADDRESS":      c.HTTP.Address,
		"JWT_SECRET_KEY":    c.JWTSecret,
		"REDIS_PASSWORD":    c.Redis.Password,
		"RABBITMQ_USER":     c.RabbitMQ.User,
		"RABBITMQ_PASSWORD": c.RabbitMQ.Password,
	}); err != nil {
		return err
	}
	if len(c.JWTSecret) < 32 {
		return errors.New("JWT_SECRET_KEY must contain at least 32 characters")
	}
	if c.DependencyInitTimeout <= 0 {
		return errors.New("DEPENDENCY_INIT_TIMEOUT must be positive")
	}
	if c.ProcessShutdownTimeout <= 0 {
		return errors.New("PROCESS_SHUTDOWN_TIMEOUT must be positive")
	}
	if c.AccessTokenTTL <= 0 {
		return errors.New("ACCESS_TOKEN_TTL must be positive")
	}
	if err := c.HTTP.validate(); err != nil {
		return err
	}
	if err := c.WebSocket.validate(); err != nil {
		return err
	}
	if err := c.MySQL.validate(); err != nil {
		return err
	}
	if c.Redis.Database < 0 {
		return errors.New("REDIS_DB must not be negative")
	}
	if err := c.RabbitMQ.validate(); err != nil {
		return err
	}
	return nil
}
