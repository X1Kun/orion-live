package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/X1Kun/orion-live/internal/loadtest"
)

func main() {
	cfg := loadtest.DefaultConfig()
	var mode string
	var outputPath string
	var connectionBaseURLs string
	if baseURL := os.Getenv("ORION_LOAD_BASE_URL"); baseURL != "" {
		cfg.BaseURL = baseURL
	}
	flag.StringVar(&mode, "mode", string(cfg.Mode), "load mode: chat or connections")
	flag.StringVar(&cfg.BaseURL, "base-url", cfg.BaseURL, "Orion HTTP base URL")
	flag.StringVar(&connectionBaseURLs, "connection-base-urls", "", "comma-separated Orion URLs used round-robin for WebSocket connections")
	flag.IntVar(&cfg.Connections, "connections", cfg.Connections, "number of WebSocket connections")
	flag.IntVar(&cfg.ConnectionsPerUser, "connections-per-user", cfg.ConnectionsPerUser, "WebSocket connections sharing one user; must not exceed the server limit")
	flag.IntVar(&cfg.MessageRate, "message-rate", cfg.MessageRate, "aggregate Chat messages sent per second")
	flag.IntVar(&cfg.Senders, "senders", 0, "number of connections sending Chat; zero uses all")
	flag.IntVar(&cfg.BurstRate, "burst-rate", 0, "optional middle phase messages per second")
	flag.DurationVar(&cfg.BurstDuration, "burst-duration", 0, "optional middle phase duration; background duration is split before and after")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "message sending duration")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "HTTP, WebSocket handshake, and write timeout")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "maximum wait for ACKs and realtime fan-out")
	flag.DurationVar(&cfg.HistoryTimeout, "history-timeout", cfg.HistoryTimeout, "maximum wait for accepted messages to persist")
	flag.Float64Var(&cfg.MaxErrorRate, "max-error-rate", cfg.MaxErrorRate, "message rejection ratio before the run fails")
	flag.StringVar(&outputPath, "output", "", "optional path for an indented JSON report")
	flag.Parse()
	cfg.Mode = loadtest.Mode(mode)
	if connectionBaseURLs != "" {
		for _, baseURL := range strings.Split(connectionBaseURLs, ",") {
			cfg.ConnectionBaseURLs = append(cfg.ConnectionBaseURLs, strings.TrimSpace(baseURL))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := loadtest.Run(ctx, cfg)
	if report.HasMeasurements() || outputPath != "" {
		if encodeErr := writeReport(report, outputPath); encodeErr != nil {
			fmt.Fprintf(os.Stderr, "write load report: %v\n", encodeErr)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "chat load failed: %v\n", err)
		os.Exit(1)
	}
}

func writeReport(report loadtest.Report, outputPath string) error {
	if report.HasMeasurements() {
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			return err
		}
	}
	if outputPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(outputPath, body, 0o644)
}
