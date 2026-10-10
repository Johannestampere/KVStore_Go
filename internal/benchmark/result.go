package benchmark

import (
	"math"
	"slices"
	"strconv"
	"time"
)

// Sample describes one attempted request, including failed attempts.
type Sample struct {
	Method  string
	Elapsed time.Duration
	Status  int
	Failure string
}

// Percentiles uses nearest-rank selection and reports milliseconds.
type Percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Summary separates successful throughput and latency from failed attempts.
type Summary struct {
	Attempted           int            `json:"attempted"`
	Succeeded           int            `json:"succeeded"`
	Failed              int            `json:"failed"`
	ErrorRate           float64        `json:"error_rate"`
	SuccessfulPerSecond float64        `json:"successful_ops_per_second"`
	LatencyMS           *Percentiles   `json:"all_latency_ms"`
	SuccessfulLatencyMS *Percentiles   `json:"successful_latency_ms"`
	Failures            map[string]int `json:"failures"`
	StatusCodes         map[string]int `json:"status_codes"`
}

// Result reports aggregate and per-method measurements over one wall-clock interval.
type Result struct {
	ElapsedSeconds float64            `json:"elapsed_seconds"`
	Total          Summary            `json:"total"`
	Methods        map[string]Summary `json:"methods"`
}

// Summarize excludes preparation time when elapsed covers only workload execution.
func Summarize(samples []Sample, elapsed time.Duration) Result {
	result := Result{
		ElapsedSeconds: elapsed.Seconds(),
		Total:          summarizeSamples(samples, elapsed),
		Methods:        make(map[string]Summary),
	}
	byMethod := make(map[string][]Sample)
	for _, sample := range samples {
		byMethod[sample.Method] = append(byMethod[sample.Method], sample)
	}
	for method, observations := range byMethod {
		result.Methods[method] = summarizeSamples(observations, elapsed)
	}
	return result
}

func summarizeSamples(samples []Sample, elapsed time.Duration) Summary {
	summary := Summary{
		Attempted: len(samples), Failures: make(map[string]int), StatusCodes: make(map[string]int),
	}
	latencies := make([]time.Duration, 0, len(samples))
	successful := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		latencies = append(latencies, sample.Elapsed)
		if sample.Status != 0 {
			summary.StatusCodes[strconv.Itoa(sample.Status)]++
		}
		if sample.Failure != "" {
			summary.Failed++
			summary.Failures[sample.Failure]++
		} else {
			summary.Succeeded++
			successful = append(successful, sample.Elapsed)
		}
	}
	if summary.Attempted > 0 {
		summary.ErrorRate = float64(summary.Failed) / float64(summary.Attempted)
	}
	if elapsed > 0 {
		summary.SuccessfulPerSecond = float64(summary.Succeeded) / elapsed.Seconds()
	}
	summary.LatencyMS = percentiles(latencies)
	summary.SuccessfulLatencyMS = percentiles(successful)
	return summary
}

func percentiles(latencies []time.Duration) *Percentiles {
	if len(latencies) == 0 {
		return nil
	}
	slices.Sort(latencies)
	value := func(quantile float64) float64 {
		index := int(math.Ceil(quantile*float64(len(latencies)))) - 1
		return float64(latencies[index]) / float64(time.Millisecond)
	}
	return &Percentiles{P50: value(0.50), P95: value(0.95), P99: value(0.99)}
}
