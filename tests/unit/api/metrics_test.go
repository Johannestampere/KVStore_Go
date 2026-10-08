package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/api"
	"kvstore/internal/metrics"
)

func TestMetricsHandler(t *testing.T) {
	registry := &metrics.Registry{}
	handler := api.NewMetricsHandler(registry)
	for _, method := range []string{"GET", "HEAD", "POST"} {
		t.Run(method, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(method, "/metrics", nil))
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("scrapes can be cached")
			}
			if method == "POST" {
				if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
					t.Fatalf("unsupported method: %+v", response)
				}
				return
			}
			if response.Code != 200 || response.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
				t.Fatalf("scrape response: %+v", response)
			}
			if method == "HEAD" {
				if response.Body.Len() != 0 {
					t.Fatal("HEAD contains a body")
				}
				return
			}
			if !strings.Contains(response.Body.String(), "# TYPE kvstore_quorum_failures_total counter\n") {
				t.Fatal("missing metric metadata")
			}
			if strings.Contains(response.Body.String(), "kvstore_http_requests_total{") {
				t.Fatal("scrape counted itself as a request")
			}
		})
	}
}
