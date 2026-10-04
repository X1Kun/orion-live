package loadtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

const setupConcurrency = 8

func Run(ctx context.Context, cfg Config) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	api, err := newAPIClient(cfg.BaseURL, cfg.RequestTimeout)
	if err != nil {
		return Report{}, err
	}
	runID, err := randomHex(6)
	if err != nil {
		return Report{}, err
	}
	password := "load-test-" + runID
	hostToken, err := api.registerAndLogin(ctx, "load-host-"+runID, password)
	if err != nil {
		return Report{}, err
	}
	sessionID, err := api.createAndStartSession(ctx, hostToken)
	if err != nil {
		return Report{}, err
	}
	defer func() {
		endCtx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
		defer cancel()
		_ = api.endSession(endCtx, hostToken, sessionID)
	}()

	userCount := (cfg.Connections + cfg.ConnectionsPerUser - 1) / cfg.ConnectionsPerUser
	tokens, err := createUsers(ctx, api, runID, password, userCount)
	if err != nil {
		return Report{}, err
	}
	results := newCollector(cfg.Connections)
	clients, err := connectClients(ctx, api, sessionID, tokens, cfg, results)
	if err != nil {
		closeClients(clients)
		return Report{}, err
	}

	readerCtx, stopReaders := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	readers.Add(len(clients))
	for _, client := range clients {
		go client.read(readerCtx, &readers)
	}
	defer func() {
		stopReaders()
		closeClients(clients)
		readers.Wait()
	}()

	startedAt := time.Now()
	if err := sendLoad(ctx, cfg, clients, results); err != nil {
		return results.report(cfg, sessionID, startedAt, time.Now(), time.Now(), persistenceResult{}), err
	}
	sendFinishedAt := time.Now()
	if err := waitForDelivery(ctx, cfg.DrainTimeout, cfg.TargetMessages(), results); err != nil {
		return results.report(cfg, sessionID, startedAt, sendFinishedAt, time.Now(), persistenceResult{}), err
	}

	accepted := results.acceptedIDs()
	rejected := results.rejectedIDs()
	persistence, err := waitForHistory(ctx, api, hostToken, sessionID, accepted, rejected, cfg.HistoryTimeout)
	finishedAt := time.Now()
	stopReaders()
	closeClients(clients)
	readers.Wait()
	report := results.report(cfg, sessionID, startedAt, sendFinishedAt, finishedAt, persistence)
	if err != nil {
		return report, err
	}
	if err := validateReport(report, cfg.MaxErrorRate); err != nil {
		return report, err
	}
	return report, nil
}

func createUsers(ctx context.Context, api *apiClient, runID, password string, count int) ([]string, error) {
	tokens := make([]string, count)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workerCount := min(count, setupConcurrency)
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				token, err := api.registerAndLogin(workerCtx, fmt.Sprintf("load-%s-%d", runID, index), password)
				if err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				tokens[index] = token
			}
		}()
	}
sendJobs:
	for index := range count {
		select {
		case jobs <- index:
		case <-workerCtx.Done():
			break sendJobs
		}
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tokens, nil
}

func connectClients(ctx context.Context, api *apiClient, sessionID uint64, tokens []string, cfg Config, results *collector) ([]*loadClient, error) {
	clients := make([]*loadClient, cfg.Connections)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	workerCount := min(cfg.Connections, 32)
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				token := tokens[index/cfg.ConnectionsPerUser]
				client, err := connectClient(workerCtx, api, sessionID, token, index, cfg.RequestTimeout, results)
				if err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				clients[index] = client
			}
		}()
	}
sendJobs:
	for index := range cfg.Connections {
		select {
		case jobs <- index:
		case <-workerCtx.Done():
			break sendJobs
		}
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-errCh:
		return clients, err
	default:
	}
	if err := ctx.Err(); err != nil {
		return clients, err
	}
	return clients, nil
}

func sendLoad(ctx context.Context, cfg Config, clients []*loadClient, results *collector) error {
	interval := time.Second / time.Duration(cfg.MessageRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for index := range cfg.TargetMessages() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		messageID, err := newMessageID()
		if err != nil {
			return err
		}
		sentAt := time.Now()
		results.recordSent(messageID, sentAt)
		if err := clients[index%len(clients)].send(messageID, fmt.Sprintf("load message %d", index)); err != nil {
			return fmt.Errorf("send message %d: %w", index, err)
		}
	}
	return nil
}

func waitForDelivery(ctx context.Context, timeout time.Duration, target int, results *collector) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		complete, err := results.progress(target)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errors.New("timed out waiting for ACK and realtime delivery")
		case <-ticker.C:
		}
	}
}

type persistenceResult struct {
	total    int
	rejected int
}

func waitForHistory(ctx context.Context, api *apiClient, token string, sessionID uint64, accepted, rejected map[string]struct{}, timeout time.Duration) (persistenceResult, error) {
	deadline := time.Now().Add(timeout)
	for {
		messages, err := api.allHistory(ctx, token, sessionID)
		if err != nil {
			return persistenceResult{}, fmt.Errorf("read Chat history: %w", err)
		}
		seen := make(map[string]struct{}, len(messages))
		acceptedCount := 0
		rejectedCount := 0
		for _, message := range messages {
			_, wasAccepted := accepted[message.MessageID]
			_, wasRejected := rejected[message.MessageID]
			if !wasAccepted && !wasRejected {
				return persistenceResult{total: len(messages)}, fmt.Errorf("history contained unknown message %q", message.MessageID)
			}
			if _, exists := seen[message.MessageID]; exists {
				return persistenceResult{total: len(messages)}, fmt.Errorf("history contained duplicate message %q", message.MessageID)
			}
			seen[message.MessageID] = struct{}{}
			if wasAccepted {
				acceptedCount++
			} else {
				rejectedCount++
			}
		}
		result := persistenceResult{total: len(seen), rejected: rejectedCount}
		if acceptedCount == len(accepted) {
			return result, nil
		}
		if time.Now().After(deadline) {
			return result, fmt.Errorf("timed out waiting for persistence: got %d of %d accepted messages", acceptedCount, len(accepted))
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func validateReport(report Report, maxErrorRate float64) error {
	if report.DuplicateAcks != 0 || report.DuplicateDeliveries != 0 {
		return fmt.Errorf("duplicates detected: ACKs=%d realtime=%d", report.DuplicateAcks, report.DuplicateDeliveries)
	}
	if report.RejectedDelivered != 0 || report.RejectedPersisted != 0 {
		return fmt.Errorf(
			"messages with rejected ACKs were later observed: realtime=%d persisted=%d",
			report.RejectedDelivered,
			report.RejectedPersisted,
		)
	}
	if report.RealtimeDeliveries != report.ExpectedDeliveries {
		return fmt.Errorf("realtime deliveries=%d, want %d", report.RealtimeDeliveries, report.ExpectedDeliveries)
	}
	if report.PersistedMessages != report.Accepted {
		return fmt.Errorf("persisted messages=%d, want %d", report.PersistedMessages, report.Accepted)
	}
	if report.RejectionRate > maxErrorRate {
		return fmt.Errorf("message error rate %.4f exceeded maximum %.4f", report.RejectionRate, maxErrorRate)
	}
	return nil
}

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func newMessageID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
