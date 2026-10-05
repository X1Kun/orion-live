package loadtest

import (
	"testing"
	"time"
)

func TestReportHasMeasurements(t *testing.T) {
	if (Report{}).HasMeasurements() {
		t.Fatal("zero report unexpectedly has measurements")
	}
	if !(Report{StartedAt: time.Now()}).HasMeasurements() {
		t.Fatal("started report does not have measurements")
	}
}
