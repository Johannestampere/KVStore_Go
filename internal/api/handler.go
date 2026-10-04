package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"unicode/utf8"

	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB, including JSON overhead.

// Service describes the operations required by the HTTP API.
// Implementations must support concurrent calls from request handlers.
type Service interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, bool, error)
	Delete(ctx context.Context, key string) error
}

// Handler validates HTTP requests and invokes a key-value service.
type Handler struct {
	service Service
	router  *http.ServeMux
}

type putRequest struct {
	// A pointer distinguishes an empty string from a missing or null value.
	Value *string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// NewHandler creates public key-value routes using a non-nil service.
func NewHandler(service Service) *Handler {
	handler := &Handler{service: service, router: http.NewServeMux()}
	handler.router.HandleFunc("PUT /kv/{key}", validateKey(handler.put))
	handler.router.HandleFunc("GET /kv/{key}", validateKey(handler.get))
	handler.router.HandleFunc("DELETE /kv/{key}", validateKey(handler.delete))
	return handler
}

func validateKey(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		key := request.PathValue("key")
		if !utf8.ValidString(key) {
			writeError(writer, http.StatusBadRequest, "key must be valid UTF-8")
			return
		}
		if len(key) > 4<<10 {
			writeError(writer, http.StatusRequestURITooLong, "key exceeds 4 KiB")
			return
		}
		next(writer, request)
	}
}

// ServeHTTP implements http.Handler by delegating to the configured router.
// The standard router also supports HEAD for GET routes and supplies unmatched
// route and unsupported-method responses.
func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.router.ServeHTTP(writer, request)
}

func (handler *Handler) put(writer http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(writer, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}

	body := http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes)
	payload, err := decodePutRequest(body)
	if err != nil {
		var sizeError *http.MaxBytesError
		if errors.As(err, &sizeError) {
			writeError(writer, http.StatusRequestEntityTooLarge, "request body exceeds size limit")
			return
		}
		writeError(writer, http.StatusBadRequest, "body must contain one JSON object with a string value and no unknown fields")
		return
	}

	if err := handler.service.Put(request.Context(), request.PathValue("key"), *payload.Value); err != nil {
		writeServiceError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *Handler) get(writer http.ResponseWriter, request *http.Request) {
	key := request.PathValue("key")
	value, found, err := handler.service.Get(request.Context(), key)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	if !found {
		writeError(writer, http.StatusNotFound, "key not found")
		return
	}

	writeJSON(writer, http.StatusOK, getResponse{Key: key, Value: value})
}

func (handler *Handler) delete(writer http.ResponseWriter, request *http.Request) {
	if err := handler.service.Delete(request.Context(), request.PathValue("key")); err != nil {
		writeServiceError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func writeServiceError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(writer, http.StatusGatewayTimeout, "replica request timed out")
	case errors.Is(err, context.Canceled):
		writeError(writer, http.StatusRequestTimeout, "request canceled")
	case errors.Is(err, replication.ErrUnavailable):
		writeError(writer, http.StatusServiceUnavailable, "replica unavailable")
	case errors.Is(err, replication.ErrInvalidResponse):
		writeError(writer, http.StatusBadGateway, "invalid replica response")
	case errors.Is(err, storage.ErrStaleRecord):
		writeError(writer, http.StatusConflict, "record version is stale")
	case errors.Is(err, storage.ErrVersionConflict):
		writeError(writer, http.StatusConflict, "conflicting record for version")
	default:
		slog.Error("key-value operation failed", "error", err)
		writeError(writer, http.StatusInternalServerError, "internal server error")
	}
}

func decodePutRequest(body io.Reader) (putRequest, error) {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	var payload putRequest
	if err := decoder.Decode(&payload); err != nil {
		return putRequest{}, fmt.Errorf("decode PUT body: %w", err)
	}

	// Decode reads one JSON value, so a second read must reach the end of input.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return putRequest{}, fmt.Errorf("read end of PUT body: %w", err)
		}
		return putRequest{}, errors.New("PUT body contains multiple JSON values")
	}
	if payload.Value == nil {
		return putRequest{}, errors.New("PUT value must be a non-null string")
	}
	return payload, nil
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, errorResponse{Error: message})
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	// Serialize before committing the status so encoding failures can return 500.
	encoded, err := json.Marshal(payload)
	if err != nil {
		slog.Error("encode HTTP response", "error", err)
		http.Error(writer, "internal server error", http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if _, err := writer.Write(encoded); err != nil {
		// Headers are already sent, so a second error response would be invalid.
		slog.Error("write HTTP response", "error", err)
	}
}
