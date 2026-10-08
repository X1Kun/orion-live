package loadtest

import (
	"context"
	"testing"
	"time"
)

func TestRunConfigurationFailureRecordsUnmeasuredStage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Connections = 0
	report, err := Run(context.Background(), cfg)
	if err == nil || report.Status != "failed" || report.FailureStage != "configuration" || report.PersistenceCheck != "not_checked" || report.HasMeasurements() {
		t.Fatalf("invalid config report = %#v, err=%v", report, err)
	}
}

func TestReportHasMeasurements(t *testing.T) {
	if (Report{}).HasMeasurements() {
		t.Fatal("zero report unexpectedly has measurements")
	}
	if !(Report{StartedAt: time.Now()}).HasMeasurements() {
		t.Fatal("started report does not have measurements")
	}
}
