package loadtest

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type collector struct {
	mu sync.Mutex

	sentAt           map[string]time.Time
	acknowledged     map[string]struct{}
	accepted         map[string]struct{}
	rejected         map[string]string
	delivered        []map[string]struct{}
	ackLatencies     []time.Duration
	broadcastLatency []time.Duration
	duplicateAcks    int
	duplicateEvents  int
	firstErr         error
}

func newCollector(connections int) *collector {
	delivered := make([]map[string]struct{}, connections)
	for i := range delivered {
		delivered[i] = make(map[string]struct{})
	}
	return &collector{
		sentAt:       make(map[string]time.Time),
		acknowledged: make(map[string]struct{}),
		accepted:     make(map[string]struct{}),
		rejected:     make(map[string]string),
		delivered:    delivered,
	}
}

func (c *collector) recordSent(messageID string, sentAt time.Time) {
	c.mu.Lock()
	c.sentAt[messageID] = sentAt
	c.mu.Unlock()
}

func (c *collector) recordAck(messageID, status, code string, observedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.acknowledged[messageID]; exists {
		c.duplicateAcks++
		return
	}
	sentAt, exists := c.sentAt[messageID]
	if !exists {
		c.setErrorLocked(fmt.Errorf("ACK referenced unknown message %q", messageID))
		return
	}
	c.acknowledged[messageID] = struct{}{}
	c.ackLatencies = append(c.ackLatencies, observedAt.Sub(sentAt))
	if status == "accepted" {
		c.accepted[messageID] = struct{}{}
		return
	}
	if status != "rejected" {
		c.setErrorLocked(fmt.Errorf("message %q returned unknown ACK status %q", messageID, status))
		return
	}
	c.rejected[messageID] = code
}

func (c *collector) recordEvent(clientIndex int, messageID string, observedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if clientIndex < 0 || clientIndex >= len(c.delivered) {
		c.setErrorLocked(fmt.Errorf("event referenced invalid client index %d", clientIndex))
		return
	}
	sentAt, exists := c.sentAt[messageID]
	if !exists {
		c.setErrorLocked(fmt.Errorf("realtime event referenced unknown message %q", messageID))
		return
	}
	if _, exists := c.delivered[clientIndex][messageID]; exists {
		c.duplicateEvents++
		return
	}
	c.delivered[clientIndex][messageID] = struct{}{}
	c.broadcastLatency = append(c.broadcastLatency, observedAt.Sub(sentAt))
}

func (c *collector) setError(err error) {
	c.mu.Lock()
	c.setErrorLocked(err)
	c.mu.Unlock()
}

func (c *collector) setErrorLocked(err error) {
	if c.firstErr == nil {
		c.firstErr = err
	}
}

func (c *collector) progress(target int) (complete bool, firstErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.firstErr != nil {
		return false, c.firstErr
	}
	if len(c.acknowledged) != target {
		return false, nil
	}
	for _, clientDeliveries := range c.delivered {
		for messageID := range c.accepted {
			if _, exists := clientDeliveries[messageID]; !exists {
				return false, nil
			}
		}
	}
	return true, nil
}

func (c *collector) acceptedIDs() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]struct{}, len(c.accepted))
	for messageID := range c.accepted {
		result[messageID] = struct{}{}
	}
	return result
}

func (c *collector) rejectedIDs() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]struct{}, len(c.rejected))
	for messageID := range c.rejected {
		result[messageID] = struct{}{}
	}
	return result
}

func (c *collector) report(cfg Config, sessionID uint64, startedAt, sendFinishedAt, finishedAt time.Time, persistence persistenceResult) Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	deliveryCount := 0
	unexpectedDeliveries := 0
	rejectedDelivered := make(map[string]struct{})
	for _, deliveries := range c.delivered {
		deliveryCount += len(deliveries)
		for messageID := range deliveries {
			if _, rejected := c.rejected[messageID]; rejected {
				unexpectedDeliveries++
				rejectedDelivered[messageID] = struct{}{}
			}
		}
	}
	report := Report{
		BaseURL:              cfg.BaseURL,
		LiveSessionID:        sessionID,
		Connections:          cfg.Connections,
		ConnectionsPerUser:   cfg.ConnectionsPerUser,
		TargetMessages:       cfg.TargetMessages(),
		MessageRate:          cfg.MessageRate,
		StartedAt:            startedAt.UTC(),
		ConfiguredDuration:   cfg.Duration.String(),
		SendDurationSeconds:  sendFinishedAt.Sub(startedAt).Seconds(),
		TotalDurationSeconds: finishedAt.Sub(startedAt).Seconds(),
		Sent:                 len(c.sentAt),
		Accepted:             len(c.accepted),
		Rejected:             len(c.rejected),
		RealtimeDeliveries:   deliveryCount,
		ExpectedDeliveries:   len(c.accepted) * len(c.delivered),
		UnexpectedDeliveries: unexpectedDeliveries,
		RejectedDelivered:    len(rejectedDelivered),
		PersistedMessages:    persistence.total,
		RejectedPersisted:    persistence.rejected,
		DuplicateAcks:        c.duplicateAcks,
		DuplicateDeliveries:  c.duplicateEvents,
		AckLatency:           summarizeDurations(c.ackLatencies),
		BroadcastLatency:     summarizeDurations(c.broadcastLatency),
		RejectionsByCode:     countValues(c.rejected),
	}
	if sendDuration := sendFinishedAt.Sub(startedAt).Seconds(); sendDuration > 0 {
		report.AchievedMessageRate = float64(len(c.sentAt)) / sendDuration
	}
	if len(c.sentAt) > 0 {
		report.RejectionRate = float64(len(c.rejected)) / float64(len(c.sentAt))
	}
	return report
}

func countValues(values map[string]string) map[string]int {
	counts := make(map[string]int)
	for _, value := range values {
		if value == "" {
			value = "unknown"
		}
		counts[value]++
	}
	return counts
}

func summarizeDurations(values []time.Duration) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return LatencySummary{
		Samples:         len(sorted),
		P50Milliseconds: durationMilliseconds(percentile(sorted, 0.50)),
		P95Milliseconds: durationMilliseconds(percentile(sorted, 0.95)),
		P99Milliseconds: durationMilliseconds(percentile(sorted, 0.99)),
		MaxMilliseconds: durationMilliseconds(sorted[len(sorted)-1]),
	}
}

func durationMilliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	index := int(math.Ceil(float64(len(sorted))*quantile)) - 1
	return sorted[index]
}
