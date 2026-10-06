package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"kvstore/internal/api"
)

func TestHealthHandler(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.HealthHandler{}.ServeHTTP(response, httptest.NewRequest(method, "/health", nil))
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Error("health responses must not be cached")
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Error("response must be JSON")
			}
			if method != http.MethodGet && method != http.MethodHead {
				if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
					t.Fatalf("unsupported method: %d, Allow=%q", response.Code, response.Header().Get("Allow"))
				}
				return
			}
			if response.Code != http.StatusOK {
				t.Fatalf("status: %d", response.Code)
			}
			if method == http.MethodHead {
				if response.Body.Len() != 0 {
					t.Fatal("HEAD returned a body")
				}
				return
			}
			var payload map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload) != 1 || payload["status"] != "ok" {
				t.Fatalf("health response: %v", payload)
			}
		})
	}
}
