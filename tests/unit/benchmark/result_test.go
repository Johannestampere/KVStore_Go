package benchmark_test

import (
	"encoding/json"
	"testing"
	"time"

	"kvstore/internal/benchmark"
)

func TestSummarySeparatesFailuresAndSuccessfulLatency(t *testing.T) {
	samples := []benchmark.Sample{
		{Method: "GET", Elapsed: 10 * time.Millisecond, Status: 200},
		{Method: "GET", Elapsed: time.Millisecond, Status: 503, Failure: "http_status"},
		{Method: "PUT", Elapsed: 30 * time.Millisecond, Status: 204},
		{Method: "PUT", Elapsed: 2 * time.Second, Failure: "timeout"},
	}
	result := benchmark.Summarize(samples, 4*time.Second)
	total := result.Total
	if total.Attempted != 4 || total.Succeeded != 2 || total.Failed != 2 || total.ErrorRate != 0.5 || total.SuccessfulPerSecond != 0.5 {
		t.Fatalf("incorrect aggregate: %+v", total)
	}
	if *total.LatencyMS != (benchmark.Percentiles{P50: 10, P95: 2000, P99: 2000}) {
		t.Fatalf("incorrect all-attempt percentiles: %+v", total.LatencyMS)
	}
	if *total.SuccessfulLatencyMS != (benchmark.Percentiles{P50: 10, P95: 30, P99: 30}) {
		t.Fatalf("incorrect successful percentiles: %+v", total.SuccessfulLatencyMS)
	}
	if total.Failures["timeout"] != 1 || total.Failures["http_status"] != 1 || total.StatusCodes["503"] != 1 || len(total.StatusCodes) != 3 {
		t.Fatalf("incorrect failure breakdown: %+v", total)
	}
	if result.Methods["GET"].SuccessfulPerSecond != 0.25 || result.Methods["PUT"].Failed != 1 {
		t.Fatalf("incorrect per-method summary: %+v", result.Methods)
	}
	if samples[0].Elapsed != 10*time.Millisecond {
		t.Fatal("summarizing changed the input samples")
	}
}

func TestSummaryNearestRankAndMissingObservations(t *testing.T) {
	var samples []benchmark.Sample
	for milliseconds := 100; milliseconds >= 1; milliseconds-- {
		samples = append(samples, benchmark.Sample{Method: "GET", Status: 200, Elapsed: time.Duration(milliseconds) * time.Millisecond})
	}
	result := benchmark.Summarize(samples, time.Second)
	if *result.Total.LatencyMS != (benchmark.Percentiles{P50: 50, P95: 95, P99: 99}) {
		t.Fatalf("incorrect percentiles: %+v", result.Total.LatencyMS)
	}
	empty := benchmark.Summarize(nil, 0)
	if empty.Total.LatencyMS != nil || empty.Total.SuccessfulLatencyMS != nil || empty.Total.ErrorRate != 0 || empty.Total.SuccessfulPerSecond != 0 {
		t.Fatalf("empty sample produced fabricated measurements: %+v", empty)
	}
	failed := benchmark.Summarize([]benchmark.Sample{{Method: "GET", Failure: "timeout", Elapsed: time.Second}}, time.Second)
	if failed.Total.SuccessfulLatencyMS != nil || failed.Total.ErrorRate != 1 {
		t.Fatalf("failed sample produced successful latency: %+v", failed)
	}
	if _, err := json.Marshal(empty); err != nil {
		t.Fatalf("empty summary must remain valid JSON: %v", err)
	}
}
