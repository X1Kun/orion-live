package loadtest

import (
	"runtime"
	"runtime/debug"
	"time"
)

type LatencySummary struct {
	Samples         int     `json:"samples"`
	P50Milliseconds float64 `json:"p50_ms"`
	P95Milliseconds float64 `json:"p95_ms"`
	P99Milliseconds float64 `json:"p99_ms"`
	MaxMilliseconds float64 `json:"max_ms"`
}

type Report struct {
	Status               string         `json:"status"`
	FailureStage         string         `json:"failure_stage,omitempty"`
	FailureReason        string         `json:"failure_reason,omitempty"`
	PersistenceCheck     string         `json:"persistence_check"`
	Mode                 Mode           `json:"mode"`
	Runtime              RuntimeDetails `json:"runtime"`
	BaseURL              string         `json:"base_url"`
	ConnectionBaseURLs   []string       `json:"connection_base_urls"`
	LiveSessionID        uint64         `json:"live_session_id"`
	Connections          int            `json:"connections"`
	ConnectionsPerUser   int            `json:"connections_per_user"`
	TargetMessages       int            `json:"target_messages"`
	MessageRate          int            `json:"message_rate_per_second"`
	Senders              int            `json:"senders"`
	BurstRate            int            `json:"burst_rate_per_second"`
	BurstDuration        string         `json:"burst_duration"`
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

type RuntimeDetails struct {
	GoVersion   string `json:"go_version"`
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	GitRevision string `json:"git_revision,omitempty"`
	GitModified bool   `json:"git_modified"`
}

func (r Report) HasMeasurements() bool {
	return !r.StartedAt.IsZero()
}

func runtimeDetails() RuntimeDetails {
	details := RuntimeDetails{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return details
	}
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs.revision":
			details.GitRevision = setting.Value
		case "vcs.modified":
			details.GitModified = setting.Value == "true"
		}
	}
	return details
}
