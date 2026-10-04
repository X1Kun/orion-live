package loadtest

import (
	"testing"
	"time"
)

func TestCollectorRequiresAckAndFanOutForEveryAcceptedMessage(t *testing.T) {
	results := newCollector(2)
	sentAt := time.Now()
	results.recordSent("message-1", sentAt)
	results.recordEvent(0, "message-1", sentAt.Add(3*time.Millisecond))
	results.recordAck("message-1", "accepted", "", sentAt.Add(2*time.Millisecond))

	if complete, err := results.progress(1); complete || err != nil {
		t.Fatalf("progress before complete = (%v, %v), want (false, nil)", complete, err)
	}
	results.recordEvent(1, "message-1", sentAt.Add(4*time.Millisecond))
	if complete, err := results.progress(1); !complete || err != nil {
		t.Fatalf("progress after complete = (%v, %v), want (true, nil)", complete, err)
	}
}

func TestCollectorDetectsDuplicateFrames(t *testing.T) {
	results := newCollector(1)
	sentAt := time.Now()
	results.recordSent("message-1", sentAt)
	results.recordAck("message-1", "accepted", "", sentAt.Add(time.Millisecond))
	results.recordAck("message-1", "accepted", "", sentAt.Add(2*time.Millisecond))
	results.recordEvent(0, "message-1", sentAt.Add(2*time.Millisecond))
	results.recordEvent(0, "message-1", sentAt.Add(3*time.Millisecond))

	report := results.report(DefaultConfig(), 1, sentAt, sentAt.Add(time.Second), sentAt.Add(2*time.Second), persistenceResult{total: 1})
	if report.DuplicateAcks != 1 || report.DuplicateDeliveries != 1 {
		t.Fatalf("duplicate counts = ACKs:%d deliveries:%d, want 1 and 1", report.DuplicateAcks, report.DuplicateDeliveries)
	}
	if err := validateReport(report, 0); err == nil {
		t.Fatal("duplicate frames were accepted")
	}
}

func TestCollectorReportsDeliveryAfterRejectedAck(t *testing.T) {
	results := newCollector(1)
	sentAt := time.Now()
	results.recordSent("message-1", sentAt)
	results.recordAck("message-1", "rejected", "CHAT_UNAVAILABLE", sentAt.Add(time.Second))
	results.recordEvent(0, "message-1", sentAt.Add(2*time.Second))

	report := results.report(
		DefaultConfig(), 1, sentAt, sentAt.Add(time.Second), sentAt.Add(2*time.Second),
		persistenceResult{total: 1, rejected: 1},
	)
	if report.RejectedDelivered != 1 || report.UnexpectedDeliveries != 1 || report.RejectedPersisted != 1 {
		t.Fatalf("ambiguous outcome report = %#v", report)
	}
	if err := validateReport(report, 1); err == nil {
		t.Fatal("delivery after rejected ACK was accepted")
	}
}

func TestSummarizeDurations(t *testing.T) {
	summary := summarizeDurations([]time.Duration{10 * time.Millisecond, time.Millisecond, 5 * time.Millisecond})
	if summary.Samples != 3 || summary.P50Milliseconds != 5 || summary.P95Milliseconds != 10 || summary.MaxMilliseconds != 10 {
		t.Fatalf("summarizeDurations() = %#v", summary)
	}
}
