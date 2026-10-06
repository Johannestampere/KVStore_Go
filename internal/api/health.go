package api

import "net/http"

// HealthHandler reports process liveness without checking storage or peers.
type HealthHandler struct{}

// ServeHTTP handles GET and HEAD health probes.
func (HealthHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if request.Method == http.MethodHead {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}
