package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB, including JSON overhead.

// Store describes the storage operations required by the HTTP API.
// Implementations must support concurrent calls from request handlers.
type Store interface {
	Put(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
}

// Handler routes HTTP requests to a store. Construct it with NewHandler.
type Handler struct {
	store  Store
	router *http.ServeMux
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

// NewHandler creates the key-value routes using a non-nil store.
func NewHandler(store Store) *Handler {
	handler := &Handler{
		store: store,
		router: http.NewServeMux(),
	}
	handler.router.HandleFunc("PUT /kv/{key}", handler.put)
	handler.router.HandleFunc("GET /kv/{key}", handler.get)
	handler.router.HandleFunc("DELETE /kv/{key}", handler.delete)
	return handler
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
			writeError(writer, http.StatusRequestEntityTooLarge, "request body exceeds 1 MiB")
			return
		}
		writeError(writer, http.StatusBadRequest, "body must contain one JSON object with a string value and no unknown fields")
		return
	}

	handler.store.Put(request.PathValue("key"), *payload.Value)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *Handler) get(writer http.ResponseWriter, request *http.Request) {
	key := request.PathValue("key")
	value, found := handler.store.Get(key)
	if !found {
		writeError(writer, http.StatusNotFound, "key not found")
		return
	}

	writeJSON(writer, http.StatusOK, getResponse{Key: key, Value: value})
}

func (handler *Handler) delete(writer http.ResponseWriter, request *http.Request) {
	handler.store.Delete(request.PathValue("key"))
	writer.WriteHeader(http.StatusNoContent)
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
