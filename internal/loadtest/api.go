package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type apiClient struct {
	baseURL *url.URL
	http    *http.Client
}

type historyMessage struct {
	ID        uint64 `json:"id"`
	MessageID string `json:"message_id"`
}

type historyPage struct {
	Data []historyMessage `json:"data"`
	Page struct {
		NextCursor uint64 `json:"next_cursor"`
		HasMore    bool   `json:"has_more"`
	} `json:"page"`
}

func newAPIClient(rawBaseURL string, timeout time.Duration) (*apiClient, error) {
	baseURL, err := url.Parse(strings.TrimRight(rawBaseURL, "/"))
	if err != nil {
		return nil, err
	}
	return &apiClient{baseURL: baseURL, http: &http.Client{Timeout: timeout}}, nil
}

func (c *apiClient) registerAndLogin(ctx context.Context, username, password string) (string, error) {
	credentials := map[string]string{"username": username, "password": password}
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/users/register", "", credentials, nil); err != nil {
		return "", fmt.Errorf("register %s: %w", username, err)
	}
	var response struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/users/login", "", credentials, &response); err != nil {
		return "", fmt.Errorf("login %s: %w", username, err)
	}
	if response.Data.AccessToken == "" {
		return "", fmt.Errorf("login %s returned an empty access token", username)
	}
	return response.Data.AccessToken, nil
}

func (c *apiClient) createAndStartSession(ctx context.Context, token string) (uint64, error) {
	var response struct {
		Data struct {
			ID uint64 `json:"id"`
		} `json:"data"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/live-sessions", token, map[string]string{"title": "Chat load baseline"}, &response); err != nil {
		return 0, fmt.Errorf("create live session: %w", err)
	}
	path := "/api/v1/live-sessions/" + strconv.FormatUint(response.Data.ID, 10) + "/start"
	if err := c.doJSON(ctx, http.MethodPost, path, token, nil, nil); err != nil {
		return 0, fmt.Errorf("start live session: %w", err)
	}
	return response.Data.ID, nil
}

func (c *apiClient) endSession(ctx context.Context, token string, sessionID uint64) error {
	path := "/api/v1/live-sessions/" + strconv.FormatUint(sessionID, 10) + "/end"
	return c.doJSON(ctx, http.MethodPost, path, token, nil, nil)
}

func (c *apiClient) allHistory(ctx context.Context, token string, sessionID uint64) ([]historyMessage, error) {
	var messages []historyMessage
	var cursor uint64
	for {
		path := fmt.Sprintf("/api/v1/live-sessions/%d/messages?after_id=%d&limit=100", sessionID, cursor)
		var page historyPage
		if err := c.doJSON(ctx, http.MethodGet, path, token, nil, &page); err != nil {
			return nil, err
		}
		messages = append(messages, page.Data...)
		if !page.Page.HasMore {
			return messages, nil
		}
		if page.Page.NextCursor <= cursor {
			return nil, errors.New("history next cursor did not advance")
		}
		cursor = page.Page.NextCursor
	}
}

func (c *apiClient) webSocketURL(sessionID uint64) string {
	endpoint := *c.baseURL
	if endpoint.Scheme == "https" {
		endpoint.Scheme = "wss"
	} else {
		endpoint.Scheme = "ws"
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/v1/live-sessions/" + strconv.FormatUint(sessionID, 10) + "/ws"
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	return endpoint.String()
}

func (c *apiClient) doJSON(ctx context.Context, method, path, token string, requestBody, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawQuery = ""
	if index := strings.IndexByte(path, '?'); index >= 0 {
		endpoint.Path = strings.TrimRight(c.baseURL.Path, "/") + path[:index]
		endpoint.RawQuery = path[index+1:]
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return err
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s %s returned %s: %s", method, path, response.Status, strings.TrimSpace(string(responseBody)))
	}
	if responseBody == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(responseBody); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}
