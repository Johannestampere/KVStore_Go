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
	"kvstore/internal/replication"
	"kvstore/internal/storage"
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

// Apply sends an unchanged record; only an accepted record or identical retry succeeds.
func (client *HTTPNodeClient) Apply(ctx context.Context, member cluster.Member, record storage.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode replica record: %w", err)
	}
	request, err := peerRequest(ctx, member, http.MethodPut, record.Key, payload)
	if err != nil {
		return err
	}
	_, _, err = client.execute(request, http.StatusNoContent)
	return err
}

// GetRecord reads a complete record, including deletion markers.
func (client *HTTPNodeClient) GetRecord(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
	request, err := peerRequest(ctx, member, http.MethodGet, key, nil)
	if err != nil {
		return storage.Record{}, false, err
	}
	body, status, err := client.execute(request, http.StatusOK)
	if err != nil {
		return storage.Record{}, false, err
	}
	if status == http.StatusNotFound {
		var missing struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &missing); err != nil || missing.Error != "key not found" {
			return storage.Record{}, false, fmt.Errorf("%w: invalid missing-key response", replication.ErrInvalidResponse)
		}
		return storage.Record{}, false, nil
	}
	record, err := replication.DecodeRecord(bytes.NewReader(body))
	if err != nil || record.Key != key {
		return storage.Record{}, false, fmt.Errorf("%w: invalid record", replication.ErrInvalidResponse)
	}
	return record, true, nil
}

func peerRequest(ctx context.Context, member cluster.Member, method, key string, body []byte) (*http.Request, error) {
	escapedKey := url.PathEscape(key)
	if key == "." || key == ".." {
		escapedKey = strings.ReplaceAll(key, ".", "%2E")
	}
	target := strings.TrimSuffix(member.Address, "/") + "/internal/records/" + escapedKey
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
		return nil, 0, fmt.Errorf("%w: %w", replication.ErrUnavailable, err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("close peer response", "error", err)
		}
	}()
	if response.StatusCode == http.StatusServiceUnavailable {
		return nil, 0, replication.ErrUnavailable
	}
	if response.StatusCode == http.StatusGatewayTimeout {
		return nil, 0, context.DeadlineExceeded
	}
	missing := request.Method == http.MethodGet && response.StatusCode == http.StatusNotFound
	conflict := request.Method == http.MethodPut && response.StatusCode == http.StatusConflict
	if response.StatusCode != expected && !missing && !conflict {
		return nil, 0, fmt.Errorf("%w: HTTP %d", replication.ErrInvalidResponse, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: read body: %w", replication.ErrInvalidResponse, err)
	}
	if len(body) > maxResponseBytes {
		return nil, 0, fmt.Errorf("%w: response exceeds 8 MiB", replication.ErrInvalidResponse)
	}
	if expected == http.StatusOK || conflict {
		mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			return nil, 0, fmt.Errorf("%w: expected JSON", replication.ErrInvalidResponse)
		}
	}
	if conflict {
		var failure struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &failure); err != nil {
			return nil, 0, replication.ErrInvalidResponse
		}
		switch failure.Error {
		case "record version is stale":
			return nil, 0, storage.ErrStaleRecord
		case "conflicting record for version":
			return nil, 0, storage.ErrVersionConflict
		default:
			return nil, 0, replication.ErrInvalidResponse
		}
	}
	return body, response.StatusCode, nil
}
