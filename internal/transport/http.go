// Package transport implements inter-node HTTP communication.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/routing"
)

const maxResponseBytes = 8 << 20

// HTTPNodeClient reuses connections and bounds each peer operation.
type HTTPNodeClient struct {
	client *http.Client
}

// NewHTTPNodeClient creates a client with a positive request timeout.
func NewHTTPNodeClient(timeout time.Duration) (*HTTPNodeClient, error) {
	if timeout <= 0 {
		return nil, errors.New("peer timeout must be positive")
	}
	return &HTTPNodeClient{client: &http.Client{
		Timeout:   timeout,
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

// CloseIdleConnections releases unused peer connections.
func (client *HTTPNodeClient) CloseIdleConnections() {
	client.client.CloseIdleConnections()
}

// Put writes through the owner's internal endpoint.
func (client *HTTPNodeClient) Put(ctx context.Context, member cluster.Member, key, value string) error {
	payload, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
	if err != nil {
		return fmt.Errorf("encode peer PUT: %w", err)
	}
	request, err := peerRequest(ctx, member, http.MethodPut, key, payload)
	if err != nil {
		return err
	}
	_, _, err = client.execute(request, http.StatusNoContent)
	return err
}

// Get reads a value or an explicit missing-key response from the owner.
func (client *HTTPNodeClient) Get(ctx context.Context, member cluster.Member, key string) (string, bool, error) {
	request, err := peerRequest(ctx, member, http.MethodGet, key, nil)
	if err != nil {
		return "", false, err
	}
	body, status, err := client.execute(request, http.StatusOK)
	if err != nil {
		return "", false, err
	}
	if status == http.StatusNotFound {
		var missing struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &missing); err != nil || missing.Error != "key not found" {
			return "", false, fmt.Errorf("%w: invalid missing-key response", routing.ErrInvalidResponse)
		}
		return "", false, nil
	}
	var result struct {
		Key   *string `json:"key"`
		Value *string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.Key == nil || *result.Key != key || result.Value == nil {
		return "", false, fmt.Errorf("%w: invalid record", routing.ErrInvalidResponse)
	}
	return *result.Value, true, nil
}

// Delete removes the key through the owner's internal endpoint.
func (client *HTTPNodeClient) Delete(ctx context.Context, member cluster.Member, key string) error {
	request, err := peerRequest(ctx, member, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	_, _, err = client.execute(request, http.StatusNoContent)
	return err
}

func peerRequest(ctx context.Context, member cluster.Member, method, key string, body []byte) (*http.Request, error) {
	escapedKey := url.PathEscape(key)
	if key == "." || key == ".." {
		escapedKey = strings.ReplaceAll(key, ".", "%2E")
	}
	target := strings.TrimSuffix(member.Address, "/") + "/internal/kv/" + escapedKey
	request, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request for node %q: %w", member.ID, err)
	}
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func (client *HTTPNodeClient) execute(request *http.Request, expected int) ([]byte, int, error) {
	response, err := client.client.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: %w", routing.ErrUnavailable, err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("close peer response", "error", err)
		}
	}()
	if response.StatusCode == http.StatusServiceUnavailable {
		return nil, 0, routing.ErrUnavailable
	}
	if response.StatusCode == http.StatusGatewayTimeout {
		return nil, 0, context.DeadlineExceeded
	}
	missing := request.Method == http.MethodGet && response.StatusCode == http.StatusNotFound
	if response.StatusCode != expected && !missing {
		return nil, 0, fmt.Errorf("%w: HTTP %d", routing.ErrInvalidResponse, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: read body: %w", routing.ErrInvalidResponse, err)
	}
	if len(body) > maxResponseBytes {
		return nil, 0, fmt.Errorf("%w: response exceeds 8 MiB", routing.ErrInvalidResponse)
	}
	if expected == http.StatusOK {
		mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			return nil, 0, fmt.Errorf("%w: expected JSON", routing.ErrInvalidResponse)
		}
	}
	return body, response.StatusCode, nil
}
