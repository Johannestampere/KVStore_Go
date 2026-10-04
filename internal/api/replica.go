package api

import (
	"errors"
	"mime"
	"net/http"

	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

// ReplicaStore exposes local records without coordination or version assignment.
type ReplicaStore interface {
	GetRecord(string) (storage.Record, bool)
	Apply(storage.Record) (bool, error)
}

// ReplicaHandler serves the node-local record protocol.
type ReplicaHandler struct {
	store  ReplicaStore
	router *http.ServeMux
}

// NewReplicaHandler creates endpoints that never forward requests.
func NewReplicaHandler(store ReplicaStore) *ReplicaHandler {
	handler := &ReplicaHandler{store: store, router: http.NewServeMux()}
	handler.router.HandleFunc("GET /internal/records/{key}", validateKey(handler.get))
	handler.router.HandleFunc("PUT /internal/records/{key}", validateKey(handler.apply))
	return handler
}

// ServeHTTP dispatches a peer request to local storage.
func (handler *ReplicaHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.router.ServeHTTP(writer, request)
}

func (handler *ReplicaHandler) get(writer http.ResponseWriter, request *http.Request) {
	if err := request.Context().Err(); err != nil {
		writeServiceError(writer, err)
		return
	}
	record, found := handler.store.GetRecord(request.PathValue("key"))
	if !found {
		writeError(writer, http.StatusNotFound, "key not found")
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (handler *ReplicaHandler) apply(writer http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(writer, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	// Re-encoding a public value can expand its JSON representation sixfold.
	record, err := replication.DecodeRecord(http.MaxBytesReader(writer, request.Body, 8<<20))
	if err != nil {
		var sizeError *http.MaxBytesError
		if errors.As(err, &sizeError) {
			writeError(writer, http.StatusRequestEntityTooLarge, "record body exceeds size limit")
			return
		}
		writeError(writer, http.StatusBadRequest, "body must contain one valid complete record")
		return
	}
	if record.Key != request.PathValue("key") {
		writeError(writer, http.StatusBadRequest, "record key does not match path")
		return
	}
	if err := request.Context().Err(); err != nil {
		writeServiceError(writer, err)
		return
	}
	if _, err := handler.store.Apply(record); err != nil {
		writeServiceError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}
