package metrics

import (
	"net/http"
	"strings"
	"time"
)

// InstrumentHTTP records completed requests; mount health and metrics separately.
func (registry *Registry) InstrumentHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		response := &statusWriter{ResponseWriter: writer}
		next.ServeHTTP(response, request)
		status := response.status
		if status == 0 {
			status = http.StatusOK
		}
		registry.ObserveHTTP(requestScope(request.URL.Path), request.Method, status, time.Since(started))
	})
}

func requestScope(path string) string {
	switch {
	case strings.HasPrefix(path, "/kv/"):
		return "public"
	case strings.HasPrefix(path, "/internal/"):
		return "internal"
	default:
		return "other"
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *statusWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	if status >= 200 || status == http.StatusSwitchingProtocols {
		writer.status = status
	}
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *statusWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

// Unwrap preserves access through http.ResponseController.
func (writer *statusWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func (writer *statusWriter) FlushError() error {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(writer.ResponseWriter).Flush()
}
