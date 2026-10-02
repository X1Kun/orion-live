//go:build kubernetes

package kubernetes_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	_ "github.com/go-sql-driver/mysql"
	gorilla "github.com/gorilla/websocket"
)

const e2ePassword = "Kubernetes-e2e-password-123!"

func TestKubernetesSmoke(t *testing.T) {
	apiA := requiredEnv(t, "ORION_K8S_API_A_URL")
	apiB := requiredEnv(t, "ORION_K8S_API_B_URL")
	db := openDatabase(t, requiredEnv(t, "ORION_K8S_MYSQL_DSN"))

	t.Run("migration schema", func(t *testing.T) { assertLatestMigration(t, db) })
	t.Run("cross-instance Chat", func(t *testing.T) { testCrossInstanceChat(t, apiA, apiB, db) })
}

func TestKubernetesResilience(t *testing.T) {
	apiA := requiredEnv(t, "ORION_K8S_API_A_URL")
	apiB := requiredEnv(t, "ORION_K8S_API_B_URL")
	podA := requiredEnv(t, "ORION_K8S_API_A_POD")
	namespace := requiredEnv(t, "ORION_K8S_NAMESPACE")
	db := openDatabase(t, requiredEnv(t, "ORION_K8S_MYSQL_DSN"))
	assertLatestMigration(t, db)
	state := testCrossInstanceChat(t, apiA, apiB, db)

	t.Run("Pod replacement and cursor recovery", func(t *testing.T) {
		testPodReplacementAndReconnect(t, apiA, apiB, podA, namespace, state)
	})
	t.Run("dependency failure recovery", func(t *testing.T) {
		testDependencyFailureRecovery(t, apiB, namespace, state, db)
	})
	t.Run("PodDisruptionBudget eviction", func(t *testing.T) { testPodDisruptionBudget(t, namespace) })
}

type chatState struct {
	sessionID  uint64
	userAToken string
	userBToken string
	cursor     uint64
}

func testCrossInstanceChat(t *testing.T, apiA, apiB string, db *sql.DB) chatState {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	hostToken, _ := registerAndLogin(t, apiA, "host-"+suffix)
	userAToken, userAID := registerAndLogin(t, apiA, "client-a-"+suffix)
	userBToken, _ := registerAndLogin(t, apiB, "client-b-"+suffix)
	sessionID := createAndStartSession(t, apiA, hostToken)

	connectionA := connectWebSocket(t, apiA, sessionID, userAToken)
	defer connectionA.Close()
	connectionB := connectWebSocket(t, apiB, sessionID, userBToken)
	defer connectionB.Close()

	messageID := newUUIDv4(t)
	writeChat(t, connectionA, messageID, "cross-instance")
	readAcceptedAck(t, connectionA, messageID)
	event := readChatEvent(t, connectionB, messageID)
	history := waitForHistoryMessage(t, apiB, userBToken, sessionID, 0, messageID)

	deadline := time.Now().Add(10 * time.Second)
	for {
		var messages, inbox int
		err := db.QueryRow("SELECT COUNT(*) FROM chat_messages WHERE live_session_id = ? AND user_id = ? AND message_id = ?", sessionID, userAID, messageID).Scan(&messages)
		if err != nil {
			t.Fatalf("count Chat messages: %v", err)
		}
		if err := db.QueryRow("SELECT COUNT(*) FROM consumer_inbox WHERE consumer_name = 'chat-persistence-v1' AND event_id = ?", event.EventID).Scan(&inbox); err != nil {
			t.Fatalf("count Inbox entries: %v", err)
		}
		if messages == 1 && inbox == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages=%d inbox=%d, want 1 and 1", messages, inbox)
		}
		time.Sleep(100 * time.Millisecond)
	}

	return chatState{sessionID: sessionID, userAToken: userAToken, userBToken: userBToken, cursor: history.ID}
}

func testPodReplacementAndReconnect(t *testing.T, apiA, apiB, podA, namespace string, state chatState) {
	t.Helper()
	connectionA := connectWebSocket(t, apiA, state.sessionID, state.userAToken)
	connectionB := connectWebSocket(t, apiB, state.sessionID, state.userBToken)
	defer connectionB.Close()

	runKubectl(t, "-n", namespace, "delete", "pod", podA, "--wait=false")
	if err := connectionA.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("set Pod A connection deadline: %v", err)
	}
	if _, _, err := connectionA.ReadMessage(); err == nil {
		t.Fatal("Pod A WebSocket remained open after Pod deletion")
	}
	_ = connectionA.Close()

	messageID := newUUIDv4(t)
	sendUntilAccepted(t, connectionB, messageID, "during replacement", 15*time.Second)
	waitForHistoryMessage(t, apiB, state.userBToken, state.sessionID, state.cursor, messageID)

	reconnected := connectWebSocket(t, apiB, state.sessionID, state.userAToken)
	defer reconnected.Close()
	recovered := historyAfter(t, apiB, state.userAToken, state.sessionID, state.cursor)
	if !containsMessage(recovered.Data, messageID) {
		t.Fatalf("reconnect History did not contain message %q", messageID)
	}
	waitForReplacementPod(t, namespace, podA)
}

func testDependencyFailureRecovery(t *testing.T, apiB, namespace string, state chatState, db *sql.DB) {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	redisToken, _ := registerAndLogin(t, apiB, "redis-failure-"+suffix)
	rabbitToken, _ := registerAndLogin(t, apiB, "rabbit-failure-"+suffix)
	recoveryToken, _ := registerAndLogin(t, apiB, "recovery-"+suffix)

	t.Run("Redis fail closed and recovery", func(t *testing.T) {
		connection := connectWebSocket(t, apiB, state.sessionID, redisToken)
		defer connection.Close()
		scaleStatefulSet(t, namespace, "redis", 0)
		waitForReadiness(t, apiB, false)
		messageID := newUUIDv4(t)
		writeChat(t, connection, messageID, "redis unavailable")
		status, code := readAck(t, connection, messageID)
		if status != "rejected" || code != "CHAT_UNAVAILABLE" {
			t.Fatalf("Redis failure ACK status=%q code=%q", status, code)
		}
		scaleStatefulSet(t, namespace, "redis", 1)
		waitForReadiness(t, apiB, true)
	})

	t.Run("RabbitMQ refusal and recovery", func(t *testing.T) {
		connection := connectWebSocket(t, apiB, state.sessionID, rabbitToken)
		defer connection.Close()
		scaleStatefulSet(t, namespace, "rabbitmq", 0)
		waitForReadiness(t, apiB, false)
		messageID := newUUIDv4(t)
		writeChat(t, connection, messageID, "rabbitmq unavailable")
		status, code := readAck(t, connection, messageID)
		if status != "rejected" || code != "CHAT_UNAVAILABLE" {
			t.Fatalf("RabbitMQ failure ACK status=%q code=%q", status, code)
		}
		scaleStatefulSet(t, namespace, "rabbitmq", 1)
		waitForReadiness(t, apiB, true)
	})

	t.Run("MySQL readiness and endpoint removal", func(t *testing.T) {
		scaleStatefulSet(t, namespace, "mysql", 0)
		waitForReadiness(t, apiB, false)
		waitForServiceAddresses(t, namespace, 0)
		scaleStatefulSet(t, namespace, "mysql", 1)
		waitForReadiness(t, apiB, true)
		waitForServiceAddresses(t, namespace, 2)
	})

	recoveredID := newUUIDv4(t)
	t.Run("final persistence after recovery", func(t *testing.T) {
		connection := connectWebSocket(t, apiB, state.sessionID, recoveryToken)
		defer connection.Close()
		sendUntilAccepted(t, connection, recoveredID, "dependencies recovered", 15*time.Second)
		waitForHistoryMessage(t, apiB, recoveryToken, state.sessionID, state.cursor, recoveredID)
	})

	deadline := time.Now().Add(15 * time.Second)
	for {
		var eventID string
		err := db.QueryRow(
			"SELECT event_id FROM chat_messages WHERE live_session_id = ? AND message_id = ?",
			state.sessionID,
			recoveredID,
		).Scan(&eventID)
		if err == nil {
			var inbox int
			if err := db.QueryRow(
				"SELECT COUNT(*) FROM consumer_inbox WHERE consumer_name = 'chat-persistence-v1' AND event_id = ?",
				eventID,
			).Scan(&inbox); err != nil {
				t.Fatalf("count recovered Inbox entry: %v", err)
			}
			if inbox == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered message %q was not persisted with an Inbox entry; last database error: %v", recoveredID, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func testPodDisruptionBudget(t *testing.T, namespace string) {
	t.Helper()
	pods := readyAPIPodNames(t, namespace)
	if len(pods) != 2 {
		t.Fatalf("ready API Pods = %v, want 2", pods)
	}
	if output, err := evictPod(namespace, pods[0]); err != nil {
		t.Fatalf("evict first API Pod: %v\n%s", err, output)
	}
	output, err := evictPod(namespace, pods[1])
	if err == nil {
		t.Fatalf("second API Pod eviction unexpectedly succeeded: %s", output)
	}
	if !strings.Contains(strings.ToLower(output), "disruption budget") && !strings.Contains(output, "TooManyRequests") {
		t.Fatalf("second eviction was not rejected by PDB: %v\n%s", err, output)
	}
	waitForReplacementPod(t, namespace, pods[0])
}

func assertLatestMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	var version uint64
	if err := db.QueryRow("SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != 4 {
		t.Fatalf("schema version = %d, want 4", version)
	}
}

func registerAndLogin(t *testing.T, baseURL, username string) (string, uint64) {
	t.Helper()
	var registered struct {
		Data struct {
			ID uint64 `json:"id"`
		} `json:"data"`
	}
	doJSON(t, http.MethodPost, baseURL+"/api/v1/users/register", "", map[string]string{"username": username, "password": e2ePassword}, &registered)
	var loggedIn struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	doJSON(t, http.MethodPost, baseURL+"/api/v1/users/login", "", map[string]string{"username": username, "password": e2ePassword}, &loggedIn)
	return loggedIn.Data.AccessToken, registered.Data.ID
}

func createAndStartSession(t *testing.T, baseURL, token string) uint64 {
	t.Helper()
	var created struct {
		Data struct {
			ID uint64 `json:"id"`
		} `json:"data"`
	}
	doJSON(t, http.MethodPost, baseURL+"/api/v1/live-sessions", token, map[string]string{"title": "Kubernetes E2E"}, &created)
	doJSON(t, http.MethodPost, fmt.Sprintf("%s/api/v1/live-sessions/%d/start", baseURL, created.Data.ID), token, nil, &struct{}{})
	return created.Data.ID
}

func connectWebSocket(t *testing.T, baseURL string, sessionID uint64, token string) *gorilla.Conn {
	t.Helper()
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse API URL: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = fmt.Sprintf("/api/v1/live-sessions/%d/ws", sessionID)
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	deadline := time.Now().Add(15 * time.Second)
	var connection *gorilla.Conn
	for {
		var response *http.Response
		connection, response, err = gorilla.DefaultDialer.Dial(parsed.String(), header)
		if err == nil {
			break
		}
		if response != nil || time.Now().After(deadline) {
			t.Fatalf("connect WebSocket: %v, status=%v", err, responseStatus(response))
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := connection.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set room.ready deadline: %v", err)
	}
	_, body, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read room.ready: %v", err)
	}
	var ready struct {
		Type          string `json:"type"`
		LiveSessionID uint64 `json:"live_session_id"`
	}
	if err := json.Unmarshal(body, &ready); err != nil || ready.Type != "room.ready" || ready.LiveSessionID != sessionID {
		t.Fatalf("unexpected room.ready: %s, %v", body, err)
	}
	return connection
}

func readAcceptedAck(t *testing.T, connection *gorilla.Conn, messageID string) {
	t.Helper()
	status, code := readAck(t, connection, messageID)
	if status != "accepted" || code != "" {
		t.Fatalf("Chat ACK status=%q code=%q", status, code)
	}
}

func writeChat(t *testing.T, connection *gorilla.Conn, messageID, content string) {
	t.Helper()
	if err := connection.WriteJSON(map[string]any{"type": "chat.send", "message_id": messageID, "content": content}); err != nil {
		t.Fatalf("send Chat: %v", err)
	}
}

func sendUntilAccepted(t *testing.T, connection *gorilla.Conn, messageID, content string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		writeChat(t, connection, messageID, content)
		status, code := readAck(t, connection, messageID)
		if status == "accepted" {
			return
		}
		if status != "rejected" || code != "CHAT_UNAVAILABLE" {
			t.Fatalf("retryable Chat ACK status=%q code=%q", status, code)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Chat %q was not accepted before retry deadline", messageID)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func readAck(t *testing.T, connection *gorilla.Conn, messageID string) (string, string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := connection.SetReadDeadline(deadline); err != nil {
			t.Fatalf("set ACK deadline: %v", err)
		}
		_, body, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read Chat ACK: %v", err)
		}
		var frame map[string]any
		if err := json.Unmarshal(body, &frame); err != nil {
			continue
		}
		if frame["type"] == "chat.ack" && frame["message_id"] == messageID {
			status, _ := frame["status"].(string)
			var code string
			if detail, ok := frame["error"].(map[string]any); ok {
				code, _ = detail["code"].(string)
			}
			return status, code
		}
	}
}

func readChatEvent(t *testing.T, connection *gorilla.Conn, messageID string) messaging.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := connection.SetReadDeadline(deadline); err != nil {
			t.Fatalf("set event deadline: %v", err)
		}
		_, body, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read realtime Chat: %v", err)
		}
		var event messaging.Event
		if err := json.Unmarshal(body, &event); err != nil || event.EventType != messaging.EventTypeChatMessageAccepted {
			continue
		}
		var payload messaging.ChatMessageAcceptedPayload
		if err := json.Unmarshal(event.Payload, &payload); err == nil && payload.MessageID == messageID {
			return event
		}
	}
}

type historyResponse struct {
	Data []historyMessage `json:"data"`
	Page struct {
		NextCursor uint64 `json:"next_cursor"`
	} `json:"page"`
}

type historyMessage struct {
	ID        uint64 `json:"id"`
	MessageID string `json:"message_id"`
}

func waitForHistoryMessage(t *testing.T, baseURL, token string, sessionID, afterID uint64, messageID string) historyMessage {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		history := historyAfter(t, baseURL, token, sessionID, afterID)
		for _, message := range history.Data {
			if message.MessageID == messageID {
				return message
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("History did not contain message %q", messageID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func historyAfter(t *testing.T, baseURL, token string, sessionID, afterID uint64) historyResponse {
	t.Helper()
	var history historyResponse
	path := fmt.Sprintf("%s/api/v1/live-sessions/%d/messages?after_id=%d&limit=100", baseURL, sessionID, afterID)
	doJSON(t, http.MethodGet, path, token, nil, &history)
	return history
}

func containsMessage(messages []historyMessage, messageID string) bool {
	for _, message := range messages {
		if message.MessageID == messageID {
			return true
		}
	}
	return false
}

func doJSON(t *testing.T, method, endpoint, token string, requestBody, responseBody any) {
	t.Helper()
	var body bytes.Buffer
	if requestBody != nil {
		if err := json.NewEncoder(&body).Encode(requestBody); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	request, err := http.NewRequestWithContext(context.Background(), method, endpoint, &body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, endpoint, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure any
		_ = json.NewDecoder(response.Body).Decode(&failure)
		t.Fatalf("%s %s status=%d body=%v", method, endpoint, response.StatusCode, failure)
	}
	if responseBody != nil {
		if err := json.NewDecoder(response.Body).Decode(responseBody); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
}

func waitForReplacementPod(t *testing.T, namespace, deletedPod string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		output := runKubectl(t, "-n", namespace, "get", "pods", "-l", "app.kubernetes.io/name=orion-live,app.kubernetes.io/component=api", "-o", "json")
		var pods struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Status struct {
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal([]byte(output), &pods); err != nil {
			t.Fatalf("decode API Pods: %v", err)
		}
		ready := 0
		oldPresent := false
		for _, pod := range pods.Items {
			if pod.Metadata.Name == deletedPod {
				oldPresent = true
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == "Ready" && condition.Status == "True" {
					ready++
				}
			}
		}
		if ready == 2 && !oldPresent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ready API Pods = %d, deleted Pod still present = %v", ready, oldPresent)
		}
		time.Sleep(time.Second)
	}
}

func readyAPIPodNames(t *testing.T, namespace string) []string {
	t.Helper()
	output := runKubectl(t, "-n", namespace, "get", "pods", "-l", "app.kubernetes.io/name=orion-live,app.kubernetes.io/component=api", "-o", "json")
	var pods struct {
		Items []struct {
			Metadata struct {
				Name              string     `json:"name"`
				DeletionTimestamp *time.Time `json:"deletionTimestamp"`
			} `json:"metadata"`
			Status struct {
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(output), &pods); err != nil {
		t.Fatalf("decode API Pods: %v", err)
	}
	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp != nil {
			continue
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				names = append(names, pod.Metadata.Name)
				break
			}
		}
	}
	return names
}

func evictPod(namespace, pod string) (string, error) {
	body := fmt.Sprintf(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":%q,"namespace":%q}}`, pod, namespace)
	endpoint := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/eviction", namespace, pod)
	command := exec.Command("kubectl", "create", "--raw", endpoint, "-f", "-")
	command.Stdin = strings.NewReader(body)
	output, err := command.CombinedOutput()
	return string(output), err
}

func scaleStatefulSet(t *testing.T, namespace, name string, replicas int) {
	t.Helper()
	runKubectl(t, "-n", namespace, "scale", "statefulset/"+name, "--replicas="+strconv.Itoa(replicas))
	if replicas > 0 {
		runKubectl(t, "-n", namespace, "rollout", "status", "statefulset/"+name, "--timeout=5m")
		return
	}
	deadline := time.Now().Add(time.Minute)
	for {
		output := runKubectl(t, "-n", namespace, "get", "pods", "-l", "app.kubernetes.io/name="+name, "--no-headers", "--ignore-not-found")
		if strings.TrimSpace(output) == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("StatefulSet %s Pod did not terminate", name)
		}
		time.Sleep(time.Second)
	}
}

func waitForReadiness(t *testing.T, baseURL string, ready bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		response, err := http.Get(baseURL + "/readyz")
		current := false
		if err == nil {
			current = response.StatusCode == http.StatusOK
			_ = response.Body.Close()
		}
		if current == ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("readiness=%v, want %v, error=%v", current, ready, err)
		}
		time.Sleep(time.Second)
	}
}

func waitForServiceAddresses(t *testing.T, namespace string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		output := runKubectl(t, "-n", namespace, "get", "endpoints", "orion-api", "-o", "json")
		var endpoints struct {
			Subsets []struct {
				Addresses []json.RawMessage `json:"addresses"`
			} `json:"subsets"`
		}
		if err := json.Unmarshal([]byte(output), &endpoints); err != nil {
			t.Fatalf("decode Service endpoints: %v", err)
		}
		count := 0
		for _, subset := range endpoints.Subsets {
			count += len(subset.Addresses)
		}
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Service ready addresses = %d, want %d", count, want)
		}
		time.Sleep(time.Second)
	}
}

func runKubectl(t *testing.T, args ...string) string {
	t.Helper()
	command := exec.Command("kubectl", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func openDatabase(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping MySQL: %v", err)
	}
	return db
}

func newUUIDv4(t *testing.T) string {
	t.Helper()
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate UUID: %v", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func responseStatus(response *http.Response) any {
	if response == nil {
		return nil
	}
	return response.StatusCode
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}
