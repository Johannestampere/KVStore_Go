package api

import (
	"log/slog"
	"net/http"

	"kvstore/internal/metrics"
)

// MetricsHandler exposes node-local measurements to Prometheus.
type MetricsHandler struct {
	registry *metrics.Registry
}

// NewMetricsHandler uses a non-nil registry shared with request instrumentation.
func NewMetricsHandler(registry *metrics.Registry) *MetricsHandler {
	return &MetricsHandler{registry: registry}
}

// ServeHTTP handles GET and HEAD scrapes without recording the scrape itself.
func (handler *MetricsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if request.Method == http.MethodHead {
		writer.WriteHeader(http.StatusOK)
		return
	}
	if err := handler.registry.WritePrometheus(writer); err != nil {
		slog.Error("write metrics response", "error", err)
	}
}
