package replication_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/metrics"
	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

func assertFailureMetric(t *testing.T, registry *metrics.Registry, sample string) {
	t.Helper()
	var output strings.Builder
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains("\n"+output.String(), "\n"+sample+"\n") {
		t.Fatalf("missing %s\n%s", sample, output.String())
	}
}

func drainCoordinator(t *testing.T, coordinator *replication.Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsDistinguishObservationFailureFromWriteFailure(t *testing.T) {
	options, client, key, _ := quorumOptions(t)
	registry := &metrics.Registry{}
	options.Observer = registry
	options.ReadQuorum = 3
	client.read = func(context.Context, cluster.Member, string) (storage.Record, bool, error) {
		return storage.Record{Key: "wrong-key", Version: storage.Version{Counter: 1, NodeID: "peer"}}, true, nil
	}
	client.write = func(context.Context, cluster.Member, storage.Record) error {
		t.Error("failed observation attempted write")
		return nil
	}
	coordinator := newCoordinator(t, options)
	if err := coordinator.Put(t.Context(), key, "value"); !errors.Is(err, replication.ErrQuorumUnavailable) {
		t.Fatalf("Put: %v", err)
	}
	drainCoordinator(t, coordinator)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="read"} 1`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="write"} 0`)
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="write"} 0`)
	var output strings.Builder
	if err := registry.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), `kvstore_replica_failures_total{operation="read"} 0`) {
		t.Fatal("invalid replica response was not counted")
	}
}

func TestMetricsCountBackgroundWriteFailureAfterQuorumSuccess(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	registry := &metrics.Registry{}
	options.Observer = registry
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			close(started)
			select {
			case <-release:
				return replication.ErrUnavailable
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-started:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	coordinator := newCoordinator(t, options)
	if err := coordinator.Put(t.Context(), key, "value"); err != nil {
		t.Fatal(err)
	}
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="write"} 0`)
	releaseOnce.Do(func() { close(release) })
	drainCoordinator(t, coordinator)
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="write"} 1`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="write"} 0`)
}

func TestMetricsExcludeReadsCanceledAfterQuorum(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	registry := &metrics.Registry{}
	options.Observer = registry
	started := make(chan struct{})
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			close(started)
			<-ctx.Done()
			return storage.Record{}, false, ctx.Err()
		}
		select {
		case <-started:
			return storage.Record{}, false, nil
		case <-ctx.Done():
			return storage.Record{}, false, ctx.Err()
		}
	}
	coordinator := newCoordinator(t, options)
	if _, _, err := coordinator.Get(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	drainCoordinator(t, coordinator)
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="read"} 0`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="read"} 0`)
}

func TestMetricsDistinguishCallerCancellationFromDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller cancellation"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			options, client, key, _ := quorumOptions(t)
			registry := &metrics.Registry{}
			options.Observer = registry
			started := make(chan struct{}, 3)
			client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
				started <- struct{}{}
				<-ctx.Done()
				return storage.Record{}, false, ctx.Err()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if deadline {
				options.Timeout = 100 * time.Millisecond
			}
			coordinator := newCoordinator(t, options)
			finished := make(chan error, 1)
			go func() { _, _, err := coordinator.Get(ctx, key); finished <- err }()
			for range 3 {
				waitSignal(t, started, "replica read did not start")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-finished:
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatalf("Get: %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("request did not end")
			}
			drainCoordinator(t, coordinator)
			count := "0"
			if deadline {
				count = "1"
			}
			assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="read"} `+count)
			if !deadline {
				assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="read"} 0`)
			}
		})
	}
}

func TestMetricsCountImpossibleWriteQuorum(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	registry := &metrics.Registry{}
	options.Observer = registry
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		entered <- struct{}{}
		select {
		case <-release:
			if member.ID == owners[0] {
				return nil
			}
			return replication.ErrUnavailable
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	coordinator := newCoordinator(t, options)
	finished := make(chan error, 1)
	go func() { finished <- coordinator.Delete(t.Context(), key) }()
	for range 3 {
		waitSignal(t, entered, "replica write did not start")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-finished; !errors.Is(err, replication.ErrQuorumUnavailable) {
		t.Fatalf("Delete: %v", err)
	}
	drainCoordinator(t, coordinator)
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="write"} 2`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="write"} 1`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="read"} 0`)
}

func TestMetricsDoNotCountRecordConflictAsMissingQuorum(t *testing.T) {
	options, client, key, _ := quorumOptions(t)
	registry := &metrics.Registry{}
	options.Observer = registry
	options.ReadQuorum = 3
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		return storage.Record{Key: key, Value: member.ID, Version: storage.Version{Counter: 1, NodeID: "writer"}}, true, nil
	}
	coordinator := newCoordinator(t, options)
	if _, _, err := coordinator.Get(t.Context(), key); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("Get: %v", err)
	}
	drainCoordinator(t, coordinator)
	assertFailureMetric(t, registry, `kvstore_replica_failures_total{operation="read"} 0`)
	assertFailureMetric(t, registry, `kvstore_quorum_failures_total{phase="read"} 0`)
}
