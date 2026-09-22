package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts HTTP responses by route, method, and status.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orion_http_requests_total",
			Help: "Total number of HTTP requests.",
		},
		[]string{"path", "method", "status"},
	)

	// HTTPRequestDuration observes HTTP request latency by route and method.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orion_http_request_duration_seconds",
			Help:    "Histogram of HTTP request durations.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"path", "method"},
	)

	// WebSocketConnections tracks currently active WebSocket connections.
	WebSocketConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orion_websocket_connections",
		Help: "Current number of active WebSocket connections.",
	})

	// RealtimeEventsTotal counts process-local realtime delivery outcomes.
	RealtimeEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orion_realtime_events_total",
			Help: "Total number of realtime events handled by outcome.",
		},
		[]string{"event_type", "result"},
	)

	// RealtimeSubscriberReady reports whether the local realtime Consumer is active.
	RealtimeSubscriberReady = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orion_realtime_subscriber_ready",
		Help: "Whether the process-local realtime subscriber is ready.",
	})
)
