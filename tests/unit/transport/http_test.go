package transport_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/routing"
	"kvstore/internal/transport"
)

func TestHTTPNodeClientGetResponses(t *testing.T) {
	cases := []struct {
		name              string
		status            int
		contentType, body string
		found             bool
		err               error
	}{
		{"value", 200, "application/json", `{"key":"key","value":"value"}`, true, nil},
		{"missing", 404, "application/json", `{"error":"key not found"}`, false, nil},
		{"generic 404", 404, "text/plain", "not found", false, routing.ErrInvalidResponse},
		{"wrong error", 404, "application/json", `{"error":"route not found"}`, false, routing.ErrInvalidResponse},
		{"wrong key", 200, "application/json", `{"key":"other","value":"value"}`, false, routing.ErrInvalidResponse},
		{"missing value", 200, "application/json", `{"key":"key"}`, false, routing.ErrInvalidResponse},
		{"null value", 200, "application/json", `{"key":"key","value":null}`, false, routing.ErrInvalidResponse},
		{"invalid JSON", 200, "application/json", `{`, false, routing.ErrInvalidResponse},
		{"trailing JSON", 200, "application/json", `{"key":"key","value":"value"}{}`, false, routing.ErrInvalidResponse},
		{"wrong content type", 200, "text/plain", `{"key":"key","value":"value"}`, false, routing.ErrInvalidResponse},
		{"unexpected status", 500, "application/json", `{}`, false, routing.ErrInvalidResponse},
		{"unavailable", 503, "application/json", `{}`, false, routing.ErrUnavailable},
		{"timeout", 504, "application/json", `{}`, false, context.DeadlineExceeded},
		{"oversized response", 200, "application/json", strings.Repeat("x", (8<<20)+1), false, routing.ErrInvalidResponse},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/internal/kv/key" {
					t.Errorf("unexpected peer request: %s %s", request.Method, request.URL)
				}
				writer.Header().Set("Content-Type", testCase.contentType)
				writer.WriteHeader(testCase.status)
				// Oversized responses may be cut off when the client closes the body.
				if _, err := io.WriteString(writer, testCase.body); err != nil && len(testCase.body) <= 8<<20 {
					t.Errorf("write response: %v", err)
				}
			}))
			t.Cleanup(server.Close)
			client := newClient(t, time.Second)
			value, found, err := client.Get(t.Context(), cluster.Member{ID: "peer", Address: server.URL}, "key")
			if !errors.Is(err, testCase.err) || found != testCase.found {
				t.Fatalf("Get = (%q, %v, %v)", value, found, err)
			}
			if found && value != "value" {
				t.Errorf("value = %q", value)
			}
		})
	}
}

func TestHTTPNodeClientRejectsMutationFailuresAndRedirects(t *testing.T) {
	var redirects atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirects.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(destination.Close)
	for _, status := range []int{200, 301, 307, 400, 404, 500} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Location", destination.URL)
			writer.WriteHeader(status)
		}))
		t.Cleanup(server.Close)
		client := newClient(t, time.Second)
		member := cluster.Member{ID: "peer", Address: server.URL}
		if err := client.Put(t.Context(), member, "key", "value"); !errors.Is(err, routing.ErrInvalidResponse) {
			t.Errorf("PUT status %d: %v", status, err)
		}
		if err := client.Delete(t.Context(), member, "key"); !errors.Is(err, routing.ErrInvalidResponse) {
			t.Errorf("DELETE status %d: %v", status, err)
		}
		if status == 301 || status == 307 {
			if _, _, err := client.Get(t.Context(), member, "key"); !errors.Is(err, routing.ErrInvalidResponse) {
				t.Errorf("GET redirect: %v", err)
			}
		}
	}
	if redirects.Load() != 0 {
		t.Fatal("client followed a redirect")
	}
}

func TestHTTPNodeClientTimeoutAndCancellation(t *testing.T) {
	for _, mode := range []string{"response headers", "response body", "caller cancellation"} {
		t.Run(mode, func(t *testing.T) {
			started := make(chan struct{})
			exited := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				defer close(exited)
				if mode == "response body" {
					writer.Header().Set("Content-Type", "application/json")
					writer.WriteHeader(http.StatusOK)
					if err := http.NewResponseController(writer).Flush(); err != nil {
						t.Errorf("flush response: %v", err)
					}
				}
				close(started)
				<-request.Context().Done()
			}))
			t.Cleanup(server.Close)
			client := newClient(t, 200*time.Millisecond)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				_, _, err := client.Get(ctx, cluster.Member{ID: "peer", Address: server.URL}, "key")
				finished <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("peer request did not start")
			}
			expected := context.DeadlineExceeded
			if mode == "caller cancellation" {
				cancel()
				expected = context.Canceled
			}
			select {
			case err := <-finished:
				if !errors.Is(err, expected) {
					t.Fatalf("error = %v, want %v", err, expected)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("peer request did not stop")
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("peer did not observe cancellation")
			}
		})
	}
}

func TestHTTPNodeClientTruncatedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Content-Length", "100")
		if _, err := io.WriteString(writer, `{`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	_, _, err := newClient(t, time.Second).Get(t.Context(), cluster.Member{ID: "peer", Address: server.URL}, "key")
	if !errors.Is(err, routing.ErrInvalidResponse) {
		t.Fatalf("error = %v", err)
	}
}

func TestNewHTTPNodeClientRejectsUnboundedTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := transport.NewHTTPNodeClient(timeout); err == nil {
			t.Fatalf("accepted timeout %v", timeout)
		}
	}
}

func newClient(t *testing.T, timeout time.Duration) *transport.HTTPNodeClient {
	t.Helper()
	client, err := transport.NewHTTPNodeClient(timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}
