package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"kvstore/internal/api"
	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

type serviceCall struct {
	operation string
	key       string
	value     string
}

type serviceSpy struct {
	calls    []serviceCall
	getValue string
	getFound bool
	err      error
	ctx      context.Context
}

func (service *serviceSpy) Put(ctx context.Context, key, value string) error {
	service.ctx = ctx
	service.calls = append(service.calls, serviceCall{operation: "put", key: key, value: value})
	return service.err
}

func (service *serviceSpy) Get(ctx context.Context, key string) (string, bool, error) {
	service.ctx = ctx
	service.calls = append(service.calls, serviceCall{operation: "get", key: key})
	return service.getValue, service.getFound, service.err
}

func (service *serviceSpy) Delete(ctx context.Context, key string) error {
	service.ctx = ctx
	service.calls = append(service.calls, serviceCall{operation: "delete", key: key})
	return service.err
}

func TestHandlerServiceErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
	}{
		{"unavailable", replication.ErrUnavailable, http.StatusServiceUnavailable},
		{"quorum", replication.ErrQuorumUnavailable, http.StatusServiceUnavailable},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"canceled", context.Canceled, http.StatusRequestTimeout},
		{"invalid response", replication.ErrInvalidResponse, http.StatusBadGateway},
		{"stale", storage.ErrStaleRecord, http.StatusConflict},
		{"conflict", storage.ErrVersionConflict, http.StatusConflict},
		{"unexpected", errors.New("private failure detail"), http.StatusInternalServerError},
	}
	for _, testCase := range cases {
		for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
			t.Run(testCase.name+"/"+method, func(t *testing.T) {
				service := &serviceSpy{err: fmt.Errorf("operation failed: %w", testCase.err)}
				request := httptest.NewRequestWithContext(t.Context(), method, "/kv/key", strings.NewReader(`{"value":"value"}`))
				request.Header.Set("Content-Type", "application/json")
				response := performRequest(api.NewHandler(service), request)
				assertStatus(t, response, testCase.status)
				assertJSONError(t, response)
				if strings.Contains(response.Body.String(), "private failure detail") {
					t.Error("response leaked internal error")
				}
				if service.ctx != request.Context() {
					t.Error("request context was not propagated")
				}
			})
		}
	}
}

func TestHandlerRejectsInvalidUTF8Key(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			service := &serviceSpy{}
			request := httptest.NewRequest(method, "/kv/%FF", strings.NewReader(`{"value":"value"}`))
			request.Header.Set("Content-Type", "application/json")
			response := performRequest(api.NewHandler(service), request)
			assertStatus(t, response, http.StatusBadRequest)
			assertServiceCalls(t, service)
		})
	}
}

func TestHandlerKeySizeLimit(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		for _, size := range []int{4 << 10, (4 << 10) + 1} {
			t.Run(fmt.Sprintf("%s/%d", method, size), func(t *testing.T) {
				service := &serviceSpy{getFound: true}
				request := httptest.NewRequest(method, "/kv/"+strings.Repeat("k", size), strings.NewReader(`{"value":"value"}`))
				request.Header.Set("Content-Type", "application/json")
				response := performRequest(api.NewHandler(service), request)
				if size > 4<<10 {
					assertStatus(t, response, http.StatusRequestURITooLong)
					assertServiceCalls(t, service)
				} else if response.Code >= 400 {
					t.Fatalf("boundary key rejected: %d", response.Code)
				}
			})
		}
	}
}

func TestHandlerPut(t *testing.T) {
	cases := []struct {
		name        string
		target      string
		body        string
		contentType string
		key         string
		value       string
	}{
		{
			name: "string value", target: "/kv/user:1", body: `{"value":"Alice"}`,
			contentType: "application/json", key: "user:1", value: "Alice",
		},
		{
			name: "empty value", target: "/kv/user:1", body: `{"value":""}`,
			contentType: "application/json", key: "user:1", value: "",
		},
		{
			name: "content type parameters", target: "/kv/user:1", body: `{"value":"Alice"}`,
			contentType: "application/json; charset=utf-8", key: "user:1", value: "Alice",
		},
		{
			name: "trailing whitespace", target: "/kv/user:1", body: "{\"value\":\"Alice\"}\n\t ",
			contentType: "application/json", key: "user:1", value: "Alice",
		},
		{
			name: "escaped key and unicode value", target: "/kv/user%2F1%20name", body: `{"value":"アリス"}`,
			contentType: "application/json", key: "user/1 name", value: "アリス",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service := &serviceSpy{}
			handler := api.NewHandler(service)

			request := httptest.NewRequest(http.MethodPut, testCase.target, strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", testCase.contentType)
			response := performRequest(handler, request)

			assertStatus(t, response, http.StatusNoContent)
			if response.Body.Len() != 0 {
				t.Errorf("PUT response body = %q, want empty", response.Body.String())
			}
			assertServiceCalls(t, service, serviceCall{operation: "put", key: testCase.key, value: testCase.value})
		})
	}
}

func TestHandlerPutRejectsInvalidJSON(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "whitespace only", body: " \n\t"},
		{name: "missing value", body: `{}`},
		{name: "null value", body: `{"value":null}`},
		{name: "number value", body: `{"value":42}`},
		{name: "boolean value", body: `{"value":true}`},
		{name: "object value", body: `{"value":{}}`},
		{name: "unknown field", body: `{"value":"Alice","extra":true}`},
		{name: "truncated object", body: `{"value":"Alice"`},
		{name: "null body", body: `null`},
		{name: "array body", body: `[{"value":"Alice"}]`},
		{name: "string body", body: `"Alice"`},
		{name: "second object", body: `{"value":"Alice"}{"value":"Bob"}`},
		{name: "trailing null", body: `{"value":"Alice"}null`},
		{name: "trailing garbage", body: `{"value":"Alice"}garbage`},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service := &serviceSpy{}
			request := httptest.NewRequest(http.MethodPut, "/kv/user:1", strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", "application/json")
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, http.StatusBadRequest)
			assertJSONError(t, response)
			assertServiceCalls(t, service)
		})
	}
}

func TestHandlerPutRejectsUnsupportedContentType(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
	}{
		{name: "missing", contentType: ""},
		{name: "plain text", contentType: "text/plain"},
		{name: "XML", contentType: "application/xml"},
		{name: "malformed parameter", contentType: "application/json; charset"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service := &serviceSpy{}
			request := httptest.NewRequest(http.MethodPut, "/kv/user:1", strings.NewReader(`{"value":"Alice"}`))
			if testCase.contentType != "" {
				request.Header.Set("Content-Type", testCase.contentType)
			}
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, http.StatusUnsupportedMediaType)
			assertJSONError(t, response)
			assertServiceCalls(t, service)
		})
	}
}

func TestHandlerPutBodySizeLimit(t *testing.T) {
	const bodyLimit = 1 << 20
	const prefix = `{"value":"`
	const suffix = `"}`
	cases := []struct {
		name   string
		size   int
		status int
	}{
		{name: "below limit", size: bodyLimit - 1, status: http.StatusNoContent},
		{name: "at limit", size: bodyLimit, status: http.StatusNoContent},
		{name: "above limit", size: bodyLimit + 1, status: http.StatusRequestEntityTooLarge},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			value := strings.Repeat("a", testCase.size-len(prefix)-len(suffix))
			service := &serviceSpy{}
			request := httptest.NewRequest(http.MethodPut, "/kv/large", strings.NewReader(prefix+value+suffix))
			request.Header.Set("Content-Type", "application/json")
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, testCase.status)
			if testCase.status == http.StatusNoContent {
				assertServiceCalls(t, service, serviceCall{operation: "put", key: "large", value: value})
			} else {
				assertJSONError(t, response)
				assertServiceCalls(t, service)
			}
		})
	}
}

func TestHandlerPutRejectsOversizedTrailingWhitespace(t *testing.T) {
	service := &serviceSpy{}
	body := `{"value":"Alice"}` + strings.Repeat(" ", 1<<20)
	request := httptest.NewRequest(http.MethodPut, "/kv/user:1", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := performRequest(api.NewHandler(service), request)

	assertStatus(t, response, http.StatusRequestEntityTooLarge)
	assertJSONError(t, response)
	assertServiceCalls(t, service)
}

func TestHandlerGet(t *testing.T) {
	cases := []struct {
		name   string
		target string
		key    string
		value  string
	}{
		{name: "string value", target: "/kv/user:1", key: "user:1", value: "Alice"},
		{name: "empty value", target: "/kv/user:1", key: "user:1", value: ""},
		{name: "escaped key", target: "/kv/user%2F1%20name", key: "user/1 name", value: "Alice"},
		{name: "JSON escaping", target: "/kv/user:1", key: "user:1", value: "Alice\n\"アリス\"\\"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			service := &serviceSpy{getValue: testCase.value, getFound: true}
			request := httptest.NewRequest(http.MethodGet, testCase.target, nil)
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, http.StatusOK)
			payload := readJSONResponse(t, response)
			expected := map[string]string{"key": testCase.key, "value": testCase.value}
			if !maps.Equal(payload, expected) {
				t.Errorf("GET response = %v, want %v", payload, expected)
			}
			assertServiceCalls(t, service, serviceCall{operation: "get", key: testCase.key})
		})
	}
}

func TestHandlerGetMissingKey(t *testing.T) {
	service := &serviceSpy{}
	request := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	response := performRequest(api.NewHandler(service), request)

	assertStatus(t, response, http.StatusNotFound)
	assertJSONError(t, response)
	assertServiceCalls(t, service, serviceCall{operation: "get", key: "missing"})
}

func TestHandlerDelete(t *testing.T) {
	service := &serviceSpy{}
	handler := api.NewHandler(service)

	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodDelete, "/kv/user%2F1", nil)
		response := performRequest(handler, request)
		assertStatus(t, response, http.StatusNoContent)
		if response.Body.Len() != 0 {
			t.Errorf("DELETE response body = %q, want empty", response.Body.String())
		}
	}
	assertServiceCalls(t, service,
		serviceCall{operation: "delete", key: "user/1"},
		serviceCall{operation: "delete", key: "user/1"},
	)
}

func TestHandlerRejectsUnsupportedMethods(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPatch, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			service := &serviceSpy{}
			request := httptest.NewRequest(method, "/kv/user:1", nil)
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, http.StatusMethodNotAllowed)
			allowed := strings.Split(response.Header().Get("Allow"), ", ")
			slices.Sort(allowed)
			expected := []string{http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPut}
			if !slices.Equal(allowed, expected) {
				t.Errorf("allowed methods = %v, want %v", allowed, expected)
			}
			assertServiceCalls(t, service)
		})
	}
}

func TestHandlerRejectsUnknownPaths(t *testing.T) {
	for _, target := range []string{"/", "/unknown", "/kv", "/kv/", "/kv/user/1"} {
		t.Run(target, func(t *testing.T) {
			service := &serviceSpy{}
			request := httptest.NewRequest(http.MethodGet, target, nil)
			response := performRequest(api.NewHandler(service), request)

			assertStatus(t, response, http.StatusNotFound)
			assertServiceCalls(t, service)
		})
	}
}

func performRequest(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertStatus(t *testing.T, response *httptest.ResponseRecorder, expected int) {
	t.Helper()
	if response.Code != expected {
		t.Fatalf("status = %d, want %d; body = %.200q", response.Code, expected, response.Body.String())
	}
}

func assertServiceCalls(t *testing.T, service *serviceSpy, expected ...serviceCall) {
	t.Helper()
	if !slices.Equal(service.calls, expected) {
		t.Errorf("service calls = %.200v, want %.200v", service.calls, expected)
	}
}

func readJSONResponse(t *testing.T, response *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode JSON response: %v", err)
	}
	return payload
}

func assertJSONError(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	payload := readJSONResponse(t, response)
	if len(payload) != 1 || payload["error"] == "" {
		t.Errorf("error response = %v, want a single non-empty error message", payload)
	}
}
