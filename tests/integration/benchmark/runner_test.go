package benchmark_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/benchmark"
	"kvstore/internal/routing"
	"kvstore/internal/storage"
)

func workload(address string) benchmark.Config {
	return benchmark.Config{
		Nodes: []string{address}, Requests: 101, Concurrency: 4,
		ReadPercent: 50, ReadKeys: 7, ValueBytes: 32, Timeout: time.Second,
	}
}

func runWorkload(t *testing.T, ctx context.Context, config benchmark.Config) (benchmark.Report, error) {
	t.Helper()
	runner, err := benchmark.NewRunner(config)
	if err != nil {
		t.Fatal(err)
	}
	return runner.Run(ctx)
}

func TestRunnerPreloadsReadsAndDistributesBothMethods(t *testing.T) {
	store, err := storage.NewMemoryStore("benchmark")
	if err != nil {
		t.Fatal(err)
	}
	handler := api.NewHandler(routing.NewLocalService(store))
	var calls [2][2]atomic.Int64
	var config benchmark.Config
	for index := range 2 {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method := 0
			if r.Method == http.MethodPut {
				method = 1
			}
			calls[index][method].Add(1)
			handler.ServeHTTP(w, r)
		}))
		defer server.Close()
		if index == 0 {
			config = workload(server.URL)
		} else {
			config.Nodes = append(config.Nodes, server.URL)
		}
	}
	report, err := runWorkload(t, context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Completed || report.Preparation.Total.Succeeded != 7 || report.Measurements.Total.Succeeded != 101 || report.Measurements.Total.Failed != 0 {
		t.Fatalf("unexpected result: %+v", report)
	}
	if report.Measurements.Methods["GET"].Attempted != 50 || report.Measurements.Methods["PUT"].Attempted != 51 {
		t.Fatalf("incorrect workload mix: %+v", report.Measurements.Methods)
	}
	for index := range 2 {
		if calls[index][0].Load() != 25 || calls[index][1].Load() < 28 {
			t.Fatalf("entry node %d did not receive both methods evenly", index)
		}
	}
}

func TestRunnerReportsStatusFailuresAndRejectsIncorrectValues(t *testing.T) {
	for _, mode := range []string{"status", "wrong value", "missing value", "malformed", "redirect", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				switch mode {
				case "status":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "wrong value":
					if err := json.NewEncoder(w).Encode(map[string]string{"key": r.URL.Path[len("/kv/"):], "value": "wrong"}); err != nil {
						t.Error(err)
					}
				case "missing value":
					if err := json.NewEncoder(w).Encode(map[string]string{"key": r.URL.Path[len("/kv/"):]}); err != nil {
						t.Error(err)
					}
				case "malformed":
					if _, err := w.Write([]byte("not JSON")); err != nil {
						t.Error(err)
					}
				case "redirect":
					http.Redirect(w, r, "/redirected", http.StatusFound)
				case "oversized":
					if _, err := w.Write(make([]byte, 5000)); err != nil {
						t.Error(err)
					}
				}
			}))
			defer server.Close()
			config := workload(server.URL)
			config.Requests, config.ReadPercent, config.ValueBytes = 4, 100, 0
			report, err := runWorkload(t, context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			failure := "invalid_response"
			if mode == "status" || mode == "redirect" {
				failure = "http_status"
			}
			if !report.Completed || report.Measurements.Total.Failures[failure] != 4 || report.Measurements.Total.SuccessfulLatencyMS != nil {
				t.Fatalf("unexpected result: %+v", report.Measurements.Total)
			}
		})
	}
}

func TestRunnerStopsAfterFailedPreparation(t *testing.T) {
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			reads.Add(1)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	report, err := runWorkload(t, context.Background(), workload(server.URL))
	if err == nil || report.Completed || report.Preparation.Total.Failed != 7 || report.Measurements.Total.Attempted != 0 || reads.Load() != 0 {
		t.Fatalf("failed preload entered measured phase: report=%+v err=%v", report, err)
	}
}

func TestRunnerBoundsConcurrencyAndReturnsPartialResultsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var active atomic.Int64
	var peak atomic.Int64
	var entered atomic.Int64
	ready := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		if entered.Add(1) == 3 {
			close(ready)
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	config := workload(server.URL)
	config.ReadPercent, config.Concurrency, config.Timeout = 0, 3, 5*time.Second
	runner, err := benchmark.NewRunner(config)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	var report benchmark.Report
	var runError error
	go func() {
		defer close(finished)
		report, runError = runner.Run(ctx)
	}()
	select {
	case <-ready:
		cancel()
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("workers did not reach the server")
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not stop workers")
	}
	if !errors.Is(runError, context.Canceled) || report.Completed || report.Measurements.Total.Attempted != 3 || peak.Load() != 3 {
		t.Fatalf("incorrect cancellation: %+v err=%v peak=%d", report, runError, peak.Load())
	}
}

func TestRunnerIncludesTimeoutsAndUnavailableNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	config := workload(server.URL)
	config.ReadPercent, config.Requests, config.Timeout = 0, 2, 30*time.Millisecond
	report, err := runWorkload(t, context.Background(), config)
	if err != nil || report.Measurements.Total.Failures["timeout"] != 2 {
		t.Fatalf("timeouts not reported: %+v err=%v", report, err)
	}
	server.Close()
	report, err = runWorkload(t, context.Background(), config)
	if err != nil || report.Measurements.Total.Failures["transport"] != 2 {
		t.Fatalf("unavailable node not reported: %+v err=%v", report, err)
	}
}

func TestRunnerReadOnlyAndRepeatedRunsUseDistinctKeys(t *testing.T) {
	store, err := storage.NewMemoryStore("benchmark")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.NewHandler(routing.NewLocalService(store)))
	defer server.Close()
	config := workload(server.URL)
	config.ReadPercent, config.ValueBytes, config.Requests = 100, 0, 9
	runner, err := benchmark.NewRunner(config)
	if err != nil {
		t.Fatal(err)
	}
	var previous string
	for range 2 {
		report, err := runner.Run(context.Background())
		if err != nil || report.Measurements.Total.Succeeded != 9 || len(report.Measurements.Methods) != 1 || report.KeyPrefix == previous {
			t.Fatalf("read-only run failed: %+v err=%v", report, err)
		}
		previous = report.KeyPrefix
	}
}

func TestRunnerDeadlineIncludesReadingTheResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	config := workload(server.URL)
	config.ReadPercent, config.ReadKeys, config.Requests, config.Timeout = 100, 1, 1, 100*time.Millisecond
	report, err := runWorkload(t, context.Background(), config)
	if err != nil || report.Measurements.Total.Failures["timeout"] != 1 || report.Measurements.Total.StatusCodes["200"] != 1 {
		t.Fatalf("response body timeout not recorded: %+v err=%v", report, err)
	}
}

func TestRunnerDoesNotIssueRequestsAfterPriorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	config := workload("http://127.0.0.1:1")
	config.ReadPercent = 0
	report, err := runWorkload(t, ctx, config)
	if !errors.Is(err, context.Canceled) || report.Completed || report.Measurements.Total.Attempted != 0 || report.Measurements.Total.LatencyMS != nil {
		t.Fatalf("pre-canceled run recorded requests: %+v err=%v", report, err)
	}
}
