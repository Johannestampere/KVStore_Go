package replication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

func quorumOptions(t *testing.T) (replication.Options, *replicaClient, string, []string) {
	t.Helper()
	options, client := newOptions(t)
	options.ReadQuorum, options.WriteQuorum = 2, 2
	key := remoteKey(t, options)
	owners, err := options.Ring.GetNodes(key, 3)
	if err != nil {
		t.Fatal(err)
	}
	return options, client, key, owners
}

func TestQuorumContinuesWithOneUnavailableReplica(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			return storage.Record{}, false, replication.ErrUnavailable
		}
		record, found := client.stores[member.ID].GetRecord(key)
		return record, found, nil
	}
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			return replication.ErrUnavailable
		}
		_, err := client.stores[member.ID].Apply(record)
		return err
	}
	coordinator := newCoordinator(t, options)
	if _, found, err := coordinator.Get(t.Context(), key); err != nil || found {
		t.Fatalf("missing key: %v %v", found, err)
	}
	if err := coordinator.Put(t.Context(), key, "value"); err != nil {
		t.Fatal(err)
	}
	if value, found, err := coordinator.Get(t.Context(), key); err != nil || !found || value != "value" {
		t.Fatalf("read: %q %v %v", value, found, err)
	}
	if err := coordinator.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, found, err := coordinator.Get(t.Context(), key); err != nil || found {
		t.Fatalf("deleted key: %v %v", found, err)
	}
}

func TestReadQuorumIgnoresSlowReplicaAndCancelsIt(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	started, exited := make(chan struct{}), make(chan struct{})
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			close(started)
			<-ctx.Done()
			close(exited)
			return storage.Record{}, false, ctx.Err()
		}
		select {
		case <-started:
		case <-ctx.Done():
			return storage.Record{}, false, ctx.Err()
		}
		return storage.Record{Key: key, Value: "value", Version: storage.Version{Counter: 100, NodeID: "writer"}}, true, nil
	}
	value, found, err := newCoordinator(t, options).Get(t.Context(), key)
	if err != nil || !found || value != "value" {
		t.Fatalf("Get: %q %v %v", value, found, err)
	}
	waitSignal(t, exited, "slow read did not observe cancellation")
}

func TestImpossibleReadQuorumReturnsBeforeSlowReplica(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	started, exited := make(chan struct{}), make(chan struct{})
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			close(started)
			<-ctx.Done()
			close(exited)
			return storage.Record{}, false, ctx.Err()
		}
		select {
		case <-started:
		case <-ctx.Done():
			return storage.Record{}, false, ctx.Err()
		}
		return storage.Record{}, false, replication.ErrUnavailable
	}
	client.write = func(context.Context, cluster.Member, storage.Record) error {
		t.Error("failed observation attempted a write")
		return nil
	}
	finished := make(chan error, 1)
	coordinator := newCoordinator(t, options)
	go func() { finished <- coordinator.Put(t.Context(), key, "value") }()
	select {
	case err := <-finished:
		if !errors.Is(err, replication.ErrQuorumUnavailable) {
			t.Fatalf("Put: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waited for a replica after quorum became impossible")
	}
	waitSignal(t, exited, "impossible quorum did not cancel slow read")
}

func TestInvalidRecordsDoNotCountTowardReadQuorum(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			return storage.Record{}, false, nil
		}
		return storage.Record{Key: "wrong", Version: storage.Version{Counter: 1, NodeID: "writer"}}, true, nil
	}
	_, _, err := newCoordinator(t, options).Get(t.Context(), key)
	if !errors.Is(err, replication.ErrQuorumUnavailable) || !errors.Is(err, replication.ErrInvalidResponse) {
		t.Fatalf("Get: %v", err)
	}
}

func TestReadQuorumSelectsTombstoneOverOlderValue(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		if member.ID == owners[0] {
			return storage.Record{}, false, replication.ErrUnavailable
		}
		record := storage.Record{Key: key, Value: "old", Version: storage.Version{Counter: 7, NodeID: "writer"}}
		if member.ID == owners[1] {
			record.Value = ""
			record.Deleted = true
			record.Version.Counter = 8
		}
		return record, true, nil
	}
	if _, found, err := newCoordinator(t, options).Get(t.Context(), key); err != nil || found {
		t.Fatalf("tombstone lost: %v %v", found, err)
	}
}

func TestWriteQuorumFinishesRemainingReplicaAfterCallerReturns(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	started, release := make(chan struct{}), make(chan struct{})
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		_, err := client.stores[member.ID].Apply(record)
		return err
	}
	coordinator := newCoordinator(t, options)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := coordinator.Put(ctx, key, "value"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "third write was not attempted")
	if _, found := client.stores[owners[0]].GetRecord(key); found {
		t.Fatal("slow replica completed before release")
	}
	cancel()
	close(release)
	drainContext, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := coordinator.Shutdown(drainContext); err != nil {
		t.Fatal(err)
	}
	var expected storage.Record
	for _, id := range owners {
		record, found := client.stores[id].GetRecord(key)
		if !found {
			t.Fatalf("replica %s did not finish", id)
		}
		if expected.Version.Counter == 0 {
			expected = record
		}
		if record != expected {
			t.Fatal("background write changed record or version")
		}
	}
	if err := coordinator.Put(t.Context(), key, "new"); !errors.Is(err, replication.ErrUnavailable) {
		t.Fatalf("shutdown accepted new request: %v", err)
	}
}

func TestBackgroundWriteKeepsOriginalDeadline(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	options.Timeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	expected, _ := ctx.Deadline()
	exited := make(chan error, 1)
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(expected) {
				t.Errorf("background deadline = %v, want %v", deadline, expected)
			}
			<-ctx.Done()
			exited <- ctx.Err()
			return ctx.Err()
		}
		return nil
	}
	if err := newCoordinator(t, options).Put(ctx, key, "value"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-exited:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("background write canceled with caller: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("background write exceeded deadline")
	}
}

func TestShutdownDeadlineCancelsBackgroundWrites(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	started, exited := make(chan struct{}), make(chan struct{})
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			close(started)
			<-ctx.Done()
			close(exited)
			return ctx.Err()
		}
		return nil
	}
	coordinator := newCoordinator(t, options)
	if err := coordinator.Put(t.Context(), key, "value"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "background write did not start")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := coordinator.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown did not wait for background work: %v", err)
	}
	waitSignal(t, exited, "shutdown did not cancel outstanding write")
	if _, _, err := coordinator.Get(t.Context(), key); !errors.Is(err, replication.ErrUnavailable) {
		t.Fatalf("shutdown accepted read: %v", err)
	}
}

func TestCancellationBeforeWriteQuorumStopsOutstandingCalls(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	started, exited := make(chan struct{}, 2), make(chan struct{}, 2)
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			return nil
		}
		started <- struct{}{}
		<-ctx.Done()
		exited <- struct{}{}
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	coordinator := newCoordinator(t, options)
	go func() { finished <- coordinator.Put(ctx, key, "value") }()
	for range 2 {
		waitSignal(t, started, "write did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Put: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not return")
	}
	for range 2 {
		waitSignal(t, exited, "write did not stop")
	}
}

func TestWriteQuorumFailurePreservesAcknowledgedCopy(t *testing.T) {
	options, client, key, owners := quorumOptions(t)
	accepted := make(chan struct{})
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			_, err := client.stores[member.ID].Apply(record)
			close(accepted)
			return err
		}
		select {
		case <-accepted:
		case <-ctx.Done():
			return ctx.Err()
		}
		return replication.ErrUnavailable
	}
	err := newCoordinator(t, options).Put(t.Context(), key, "partial")
	if !errors.Is(err, replication.ErrQuorumUnavailable) {
		t.Fatalf("Put: %v", err)
	}
	if value, found := client.stores[owners[0]].Get(key); !found || value != "partial" {
		t.Fatal("failed write rolled back accepted copy")
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}
