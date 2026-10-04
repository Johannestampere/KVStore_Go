package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/routing"
	"kvstore/internal/storage"
)

func TestKeyLifecycle(t *testing.T) {
	server := newTestServer(t)
	key := "user/42 name"
	target := server.URL + "/kv/" + url.PathEscape(key)

	response := sendRequest(t, server.Client(), http.MethodGet, target, "")
	assertStatus(t, response, http.StatusNotFound)
	assertJSONBody(t, response, map[string]string{"error": "key not found"})

	for _, value := range []string{"Alice", "Bob", ""} {
		payload, err := json.Marshal(map[string]string{"value": value})
		if err != nil {
			t.Fatalf("encode PUT body: %v", err)
		}
		response = sendRequest(t, server.Client(), http.MethodPut, target, string(payload))
		assertStatus(t, response, http.StatusNoContent)
		assertEmptyBody(t, response)

		response = sendRequest(t, server.Client(), http.MethodGet, target, "")
		assertStatus(t, response, http.StatusOK)
		assertJSONBody(t, response, map[string]string{"key": key, "value": value})
	}

	for attempt := 0; attempt < 2; attempt++ {
		response = sendRequest(t, server.Client(), http.MethodDelete, target, "")
		assertStatus(t, response, http.StatusNoContent)
		assertEmptyBody(t, response)
	}

	response = sendRequest(t, server.Client(), http.MethodGet, target, "")
	assertStatus(t, response, http.StatusNotFound)
	assertJSONBody(t, response, map[string]string{"error": "key not found"})
}

func TestRejectedPutPreservesStoredValue(t *testing.T) {
	server := newTestServer(t)
	target := server.URL + "/kv/user:1"
	response := sendRequest(t, server.Client(), http.MethodPut, target, `{"value":"Alice"}`)
	assertStatus(t, response, http.StatusNoContent)
	assertEmptyBody(t, response)

	response = sendRequest(t, server.Client(), http.MethodPut, target, `{"value":"Bob"}{}`)
	assertStatus(t, response, http.StatusBadRequest)
	readBody(t, response)

	response = sendRequest(t, server.Client(), http.MethodGet, target, "")
	assertStatus(t, response, http.StatusOK)
	assertJSONBody(t, response, map[string]string{"key": "user:1", "value": "Alice"})
}

func TestConcurrentKeyLifecycles(t *testing.T) {
	server := newTestServer(t)

	for worker := 0; worker < 16; worker++ {
		t.Run(fmt.Sprintf("client_%d", worker), func(t *testing.T) {
			t.Parallel()
			key := fmt.Sprintf("user:%d", worker)
			target := server.URL + "/kv/" + key

			response := sendRequest(t, server.Client(), http.MethodPut, target, `{"value":"Alice"}`)
			assertStatus(t, response, http.StatusNoContent)
			assertEmptyBody(t, response)

			response = sendRequest(t, server.Client(), http.MethodGet, target, "")
			assertStatus(t, response, http.StatusOK)
			assertJSONBody(t, response, map[string]string{"key": key, "value": "Alice"})

			response = sendRequest(t, server.Client(), http.MethodDelete, target, "")
			assertStatus(t, response, http.StatusNoContent)
			assertEmptyBody(t, response)

			response = sendRequest(t, server.Client(), http.MethodGet, target, "")
			assertStatus(t, response, http.StatusNotFound)
			assertJSONBody(t, response, map[string]string{"error": "key not found"})
		})
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(api.NewHandler(routing.NewLocalService(storage.NewMemoryStore())))
	server.Client().Timeout = 5 * time.Second
	t.Cleanup(server.Close)
	return server
}

func sendRequest(t *testing.T, client *http.Client, method, target, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create %s request: %v", method, err)
	}
	if method == http.MethodPut {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send %s request: %v", method, err)
	}
	t.Cleanup(func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})
	return response
}

func assertStatus(t *testing.T, response *http.Response, expected int) {
	t.Helper()
	if response.StatusCode != expected {
		t.Fatalf("status = %d, want %d; body = %.200q", response.StatusCode, expected, readBody(t, response))
	}
}

func assertEmptyBody(t *testing.T, response *http.Response) {
	t.Helper()
	if body := readBody(t, response); len(body) != 0 {
		t.Errorf("response body = %q, want empty", body)
	}
}

func assertJSONBody(t *testing.T, response *http.Response, expected map[string]string) {
	t.Helper()
	if contentType := response.Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var payload map[string]string
	if err := json.Unmarshal(readBody(t, response), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !maps.Equal(payload, expected) {
		t.Errorf("response = %v, want %v", payload, expected)
	}
}

func readBody(t *testing.T, response *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return body
}
