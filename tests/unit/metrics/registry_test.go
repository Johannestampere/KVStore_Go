package metrics_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"kvstore/internal/metrics"
)

func scrape(t *testing.T, registry *metrics.Registry) string {
	t.Helper()
	var output strings.Builder
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	return output.String()
}

func requireSample(t *testing.T, output, sample string) {
	t.Helper()
	if !strings.Contains("\n"+output, "\n"+sample+"\n") {
		t.Fatalf("missing sample %s\n%s", sample, output)
	}
}

func TestRegistryCountsStatusesAndCumulativeLatencyBuckets(t *testing.T) {
	registry := &metrics.Registry{}
	registry.ObserveHTTP("public", "GET", 200, 5*time.Millisecond)
	registry.ObserveHTTP("public", "GET", 404, 50*time.Millisecond)
	registry.ObserveHTTP("public", "GET", 503, 12*time.Second)
	registry.ObserveHTTP("internal", "GET", 200, time.Millisecond)
	registry.RecordReplicaFailure("read")
	registry.RecordQuorumFailure("read")
	output := scrape(t, registry)
	for _, sample := range []string{
		`kvstore_http_requests_total{scope="public",method="GET",status="200"} 1`,
		`kvstore_http_requests_total{scope="public",method="GET",status="404"} 1`,
		`kvstore_http_requests_total{scope="public",method="GET",status="503"} 1`,
		`kvstore_http_requests_total{scope="internal",method="GET",status="200"} 1`,
		`kvstore_http_request_duration_seconds_bucket{scope="public",method="GET",le="0.001"} 0`,
		`kvstore_http_request_duration_seconds_bucket{scope="public",method="GET",le="0.005"} 1`,
		`kvstore_http_request_duration_seconds_bucket{scope="public",method="GET",le="0.05"} 2`,
		`kvstore_http_request_duration_seconds_bucket{scope="public",method="GET",le="10"} 2`,
		`kvstore_http_request_duration_seconds_bucket{scope="public",method="GET",le="+Inf"} 3`,
		`kvstore_http_request_duration_seconds_count{scope="public",method="GET"} 3`,
		`kvstore_http_request_duration_seconds_sum{scope="public",method="GET"} 12.055`,
		`kvstore_replica_failures_total{operation="read"} 1`,
		`kvstore_quorum_failures_total{phase="read"} 1`,
	} {
		requireSample(t, output, sample)
	}
	if output != scrape(t, registry) {
		t.Fatal("unchanged observations produced a different scrape")
	}
}

func TestRegistryBoundsUnknownLabels(t *testing.T) {
	registry := &metrics.Registry{}
	for index := range 100 {
		label := fmt.Sprintf("private-key-%d", index)
		registry.ObserveHTTP(label, label, 1000+index, -time.Second)
		registry.RecordReplicaFailure(label)
		registry.RecordQuorumFailure(label)
	}
	output := scrape(t, registry)
	if strings.Contains(output, "private-key") {
		t.Fatal("unbounded label escaped normalization")
	}
	requireSample(t, output, `kvstore_http_requests_total{scope="other",method="OTHER",status="other"} 100`)
	requireSample(t, output, `kvstore_http_request_duration_seconds_sum{scope="other",method="OTHER"} 0`)
	requireSample(t, output, `kvstore_replica_failures_total{operation="other"} 100`)
}

func TestRegistryConcurrentRecordingAndScraping(t *testing.T) {
	registry := &metrics.Registry{}
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			for range 100 {
				registry.ObserveHTTP("public", "PUT", 204, time.Millisecond)
				registry.RecordReplicaFailure("write")
				registry.RecordQuorumFailure("write")
			}
		})
	}
	workers.Go(func() {
		for range 100 {
			scrape(t, registry)
		}
	})
	workers.Wait()
	output := scrape(t, registry)
	requireSample(t, output, `kvstore_http_requests_total{scope="public",method="PUT",status="204"} 1200`)
	requireSample(t, output, `kvstore_http_request_duration_seconds_count{scope="public",method="PUT"} 1200`)
	requireSample(t, output, `kvstore_http_request_duration_seconds_bucket{scope="public",method="PUT",le="+Inf"} 1200`)
	requireSample(t, output, `kvstore_replica_failures_total{operation="write"} 1200`)
	requireSample(t, output, `kvstore_quorum_failures_total{phase="write"} 1200`)
}

type callbackWriter struct{ write func([]byte) (int, error) }

func (writer callbackWriter) Write(body []byte) (int, error) { return writer.write(body) }

func TestRegistryReleasesLockBeforeWritingSnapshot(t *testing.T) {
	registry := &metrics.Registry{}
	err := registry.WritePrometheus(callbackWriter{write: func(body []byte) (int, error) {
		registry.ObserveHTTP("public", "GET", 200, time.Millisecond)
		return len(body), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireSample(t, scrape(t, registry), `kvstore_http_requests_total{scope="public",method="GET",status="200"} 1`)
}

func TestRegistryPropagatesWriteErrors(t *testing.T) {
	failure := errors.New("connection closed")
	for _, test := range []struct {
		name    string
		written int
		err     error
		want    error
	}{
		{"write error", 0, failure, failure},
		{"short write", 1, nil, io.ErrShortWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := (&metrics.Registry{}).WritePrometheus(callbackWriter{write: func([]byte) (int, error) { return test.written, test.err }})
			if !errors.Is(err, test.want) {
				t.Fatalf("WritePrometheus: %v, want %v", err, test.want)
			}
		})
	}
}
