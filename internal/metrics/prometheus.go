package metrics

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// WritePrometheus exports a consistent snapshot in Prometheus text format 0.0.4.
func (registry *Registry) WritePrometheus(writer io.Writer) error {
	measurements := registry.snapshot()
	var output strings.Builder
	// In-memory formatting cannot fail; network I/O happens after releasing the lock.
	writeRequests(&output, measurements)
	writeDurations(&output, measurements)
	writeFailures(&output, "kvstore_replica_failures_total", "Failed replica calls excluding cancellation.", "operation", measurements.replicaFailures)
	writeFailures(&output, "kvstore_quorum_failures_total", "Quorum phases ending without enough responses, excluding cancellation.", "phase", measurements.quorumFailures)
	written, err := io.WriteString(writer, output.String())
	if err == nil && written != output.Len() {
		return io.ErrShortWrite
	}
	return err
}

func writeRequests(output *strings.Builder, measurements snapshot) {
	fmt.Fprintln(output, "# HELP kvstore_http_requests_total Completed HTTP handler calls by scope, method, and status.")
	fmt.Fprintln(output, "# TYPE kvstore_http_requests_total counter")
	labels := slices.Collect(maps.Keys(measurements.requests))
	slices.SortFunc(labels, func(left, right requestLabels) int {
		if order := compareHTTPLabels(left.httpLabels, right.httpLabels); order != 0 {
			return order
		}
		return cmp.Compare(left.status, right.status)
	})
	for _, label := range labels {
		status := strconv.Itoa(label.status)
		if label.status == 0 {
			status = "other"
		}
		fmt.Fprintf(output, "kvstore_http_requests_total{scope=%q,method=%q,status=%q} %d\n", label.scope, label.method, status, measurements.requests[label])
	}
}

func writeDurations(output *strings.Builder, measurements snapshot) {
	fmt.Fprintln(output, "# HELP kvstore_http_request_duration_seconds HTTP handler duration in seconds, excluding health and metrics endpoints.")
	fmt.Fprintln(output, "# TYPE kvstore_http_request_duration_seconds histogram")
	labels := slices.Collect(maps.Keys(measurements.durations))
	slices.SortFunc(labels, compareHTTPLabels)
	for _, label := range labels {
		duration := measurements.durations[label]
		for index, bound := range latencyBounds {
			fmt.Fprintf(output, "kvstore_http_request_duration_seconds_bucket{scope=%q,method=%q,le=%q} %d\n", label.scope, label.method, strconv.FormatFloat(bound, 'g', -1, 64), duration.buckets[index])
		}
		fmt.Fprintf(output, "kvstore_http_request_duration_seconds_bucket{scope=%q,method=%q,le=\"+Inf\"} %d\n", label.scope, label.method, duration.count)
		fmt.Fprintf(output, "kvstore_http_request_duration_seconds_sum{scope=%q,method=%q} %g\n", label.scope, label.method, duration.sum)
		fmt.Fprintf(output, "kvstore_http_request_duration_seconds_count{scope=%q,method=%q} %d\n", label.scope, label.method, duration.count)
	}
}

func writeFailures(output *strings.Builder, name, help, label string, counts [3]uint64) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for index, phase := range []string{"read", "write", "other"} {
		fmt.Fprintf(output, "%s{%s=%q} %d\n", name, label, phase, counts[index])
	}
}

func compareHTTPLabels(left, right httpLabels) int {
	if order := strings.Compare(left.scope, right.scope); order != 0 {
		return order
	}
	return strings.Compare(left.method, right.method)
}
