package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/X1Kun/orion-live/internal/loadtest"
)

func main() {
	cfg := loadtest.DefaultConfig()
	if baseURL := os.Getenv("ORION_LOAD_BASE_URL"); baseURL != "" {
		cfg.BaseURL = baseURL
	}
	flag.StringVar(&cfg.BaseURL, "base-url", cfg.BaseURL, "Orion HTTP base URL")
	flag.IntVar(&cfg.Connections, "connections", cfg.Connections, "number of WebSocket connections")
	flag.IntVar(&cfg.ConnectionsPerUser, "connections-per-user", cfg.ConnectionsPerUser, "WebSocket connections sharing one user; must not exceed the server limit")
	flag.IntVar(&cfg.MessageRate, "message-rate", cfg.MessageRate, "aggregate Chat messages sent per second")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "message sending duration")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "HTTP, WebSocket handshake, and write timeout")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "maximum wait for ACKs and realtime fan-out")
	flag.DurationVar(&cfg.HistoryTimeout, "history-timeout", cfg.HistoryTimeout, "maximum wait for accepted messages to persist")
	flag.Float64Var(&cfg.MaxErrorRate, "max-error-rate", cfg.MaxErrorRate, "message rejection ratio before the run fails")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := loadtest.Run(ctx, cfg)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		fmt.Fprintf(os.Stderr, "encode load report: %v\n", encodeErr)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "chat load failed: %v\n", err)
		os.Exit(1)
	}
}
