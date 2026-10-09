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

	OutboxPublishTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "orion_outbox_publish_total", Help: "Total number of Outbox publish outcomes."},
		[]string{"event_type", "result"},
	)

	OutboxFencingFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orion_outbox_fencing_failures_total",
		Help: "Total number of Outbox state updates rejected by claim fencing.",
	})

	ChatAdmissionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "orion_chat_admission_total", Help: "Total number of Chat admission decisions."},
		[]string{"result"},
	)

	PersistenceEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "orion_persistence_events_total", Help: "Total number of persistence events handled by outcome."},
		[]string{"result"},
	)

	PersistenceConsumerReady = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orion_persistence_consumer_ready",
		Help: "Whether the durable persistence consumer is ready.",
	})

	ChatConflictsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orion_chat_conflicts_total",
		Help: "Total number of conflicting Chat messages rejected by the business key.",
	})

	RabbitMQPublishTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "orion_rabbitmq_publish_total", Help: "Total number of RabbitMQ publish outcomes."},
		[]string{"event_type", "result"},
	)

	RabbitMQPublishDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orion_rabbitmq_publish_duration_seconds",
			Help:    "RabbitMQ publication duration including Publisher Confirm wait.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"event_type", "result"},
	)

	ChatAdmissionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orion_chat_admission_duration_seconds",
			Help:    "Redis Chat admission decision duration.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"result"},
	)

	RabbitMQPublishAcquireDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "orion_rabbitmq_publish_acquire_duration_seconds",
		Help:    "Time waiting to acquire a Publisher lane, including failed acquisitions.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})

	WebSocketFrameProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "orion_websocket_frame_processing_duration_seconds",
		Help:    "Application processing time for one WebSocket frame before the next read.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	WebSocketQueueWait = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orion_websocket_queue_wait_seconds",
		Help:    "Wait from successful queue admission until dequeue; outbound includes ACKs and realtime frames. Excludes socket write duration and discarded frames.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"direction"})

	PersistenceProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "orion_persistence_processing_duration_seconds",
			Help:    "Persistence Consumer delivery processing duration.",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"result"},
	)

	ChatPersistenceLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "orion_chat_persistence_lag_seconds",
		Help:    "Time from Chat acceptance to its first successful MySQL persistence.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})

	WebSocketSlowClientRemovals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orion_websocket_slow_client_removals_total",
		Help: "Total number of WebSocket Clients removed because their outbound queue could not accept a broadcast.",
	})
)
