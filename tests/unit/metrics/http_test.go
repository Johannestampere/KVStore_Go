package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/metrics"
)

func TestHTTPInstrumentationPreservesResponseAndFirstStatus(t *testing.T) {
	registry := &metrics.Registry{}
	handler := registry.InstrumentHTTP(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Test", "preserved")
		writer.WriteHeader(http.StatusNotFound)
		writer.WriteHeader(http.StatusInternalServerError)
		if _, err := writer.Write([]byte("missing")); err != nil {
			t.Error(err)
		}
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/kv/private-key?secret=value", nil))
	if response.Code != 404 || response.Body.String() != "missing" || response.Header().Get("X-Test") != "preserved" {
		t.Fatalf("response changed: %+v", response)
	}
	output := scrape(t, registry)
	requireSample(t, output, `kvstore_http_requests_total{scope="public",method="GET",status="404"} 1`)
	if strings.Contains(output, "private-key") || strings.Contains(output, "secret") {
		t.Fatal("request data leaked into metrics")
	}
}

func TestHTTPInstrumentationImplicitSuccessAndScopes(t *testing.T) {
	registry := &metrics.Registry{}
	handler := registry.InstrumentHTTP(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "PUT" {
			if _, err := writer.Write([]byte("ok")); err != nil {
				t.Error(err)
			}
		}
	}))
	for _, request := range []struct{ method, path string }{
		{"PUT", "/internal/records/key"}, {"HEAD", "/kv/key"}, {"POST", "/unknown"},
	} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(request.method, request.path, nil))
	}
	output := scrape(t, registry)
	requireSample(t, output, `kvstore_http_requests_total{scope="internal",method="PUT",status="200"} 1`)
	requireSample(t, output, `kvstore_http_requests_total{scope="public",method="HEAD",status="200"} 1`)
	requireSample(t, output, `kvstore_http_requests_total{scope="other",method="OTHER",status="200"} 1`)
}

func TestHTTPInstrumentationTracksFlushAndInformationalHeaders(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(map[bool]string{false: "informational", true: "flush"}[flush], func(t *testing.T) {
			registry := &metrics.Registry{}
			handler := registry.InstrumentHTTP(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if flush {
					if err := http.NewResponseController(writer).Flush(); err != nil {
						t.Error(err)
					}
				} else {
					writer.WriteHeader(http.StatusEarlyHints)
				}
				writer.WriteHeader(http.StatusNoContent)
			}))
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/kv/key", nil))
			status := "204"
			if flush {
				status = "200"
			}
			requireSample(t, scrape(t, registry), `kvstore_http_requests_total{scope="public",method="GET",status="`+status+`"} 1`)
		})
	}
}
