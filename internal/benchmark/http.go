package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

func (runner *Runner) request(ctx context.Context, client *http.Client, method, key string, ordinal int) Sample {
	started := time.Now()
	sample := Sample{Method: method}
	address := runner.config.Nodes[ordinal%len(runner.config.Nodes)] + "/kv/" + key
	var body io.Reader
	if method == http.MethodPut {
		body = bytes.NewReader(runner.body)
	}
	request, err := http.NewRequestWithContext(ctx, method, address, body)
	if err != nil {
		sample.Failure = "request"
		sample.Elapsed = time.Since(started)
		return sample
	}
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		sample.Failure = requestFailure(err)
	} else {
		sample.Status = response.StatusCode
		sample.Failure = runner.checkResponse(response, method, key)
	}
	sample.Elapsed = time.Since(started)
	return sample
}

func (runner *Runner) checkResponse(response *http.Response, method, key string) string {
	// Bound allocations even when an endpoint returns an unexpected response.
	limit := int64(runner.config.ValueBytes + 4096)
	body, readError := io.ReadAll(io.LimitReader(response.Body, limit+1))
	closeError := response.Body.Close()
	if readError != nil {
		return requestFailure(readError)
	}
	if closeError != nil {
		return "response_close"
	}
	if int64(len(body)) > limit {
		return "invalid_response"
	}
	expectedStatus := http.StatusNoContent
	if method == http.MethodGet {
		expectedStatus = http.StatusOK
	}
	if response.StatusCode != expectedStatus {
		return "http_status"
	}
	if method == http.MethodGet {
		var payload struct {
			Key   string  `json:"key"`
			Value *string `json:"value"`
		}
		if json.Unmarshal(body, &payload) != nil || payload.Key != key || payload.Value == nil || *payload.Value != runner.value {
			return "invalid_response"
		}
	}
	return ""
}

func requestFailure(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
		return "timeout"
	}
	return "transport"
}
