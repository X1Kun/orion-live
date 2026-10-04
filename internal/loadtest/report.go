package loadtest

import "time"

type LatencySummary struct {
	Samples         int     `json:"samples"`
	P50Milliseconds float64 `json:"p50_ms"`
	P95Milliseconds float64 `json:"p95_ms"`
	P99Milliseconds float64 `json:"p99_ms"`
	MaxMilliseconds float64 `json:"max_ms"`
}

type Report struct {
	BaseURL              string         `json:"base_url"`
	LiveSessionID        uint64         `json:"live_session_id"`
	Connections          int            `json:"connections"`
	ConnectionsPerUser   int            `json:"connections_per_user"`
	TargetMessages       int            `json:"target_messages"`
	MessageRate          int            `json:"message_rate_per_second"`
	AchievedMessageRate  float64        `json:"achieved_message_rate_per_second"`
	StartedAt            time.Time      `json:"started_at"`
	ConfiguredDuration   string         `json:"configured_duration"`
	SendDurationSeconds  float64        `json:"send_duration_seconds"`
	TotalDurationSeconds float64        `json:"total_duration_seconds"`
	Sent                 int            `json:"sent"`
	Accepted             int            `json:"accepted"`
	Rejected             int            `json:"rejected"`
	RejectionRate        float64        `json:"rejection_rate"`
	RealtimeDeliveries   int            `json:"realtime_deliveries"`
	ExpectedDeliveries   int            `json:"expected_deliveries"`
	UnexpectedDeliveries int            `json:"unexpected_realtime_deliveries"`
	RejectedDelivered    int            `json:"rejected_messages_delivered"`
	PersistedMessages    int            `json:"persisted_messages"`
	RejectedPersisted    int            `json:"rejected_messages_persisted"`
	DuplicateAcks        int            `json:"duplicate_acks"`
	DuplicateDeliveries  int            `json:"duplicate_deliveries"`
	AckLatency           LatencySummary `json:"ack_latency"`
	BroadcastLatency     LatencySummary `json:"broadcast_latency"`
	RejectionsByCode     map[string]int `json:"rejections_by_code,omitempty"`
}
