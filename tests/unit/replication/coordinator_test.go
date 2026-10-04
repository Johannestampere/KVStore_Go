package replication_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"kvstore/internal/cluster"
	"kvstore/internal/consistenthash"
	"kvstore/internal/replication"
	"kvstore/internal/storage"
)

type replicaClient struct {
	stores map[string]*storage.MemoryStore
	read   func(context.Context, cluster.Member, string) (storage.Record, bool, error)
	write  func(context.Context, cluster.Member, storage.Record) error
}

func (client *replicaClient) GetRecord(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
	if client.read != nil {
		return client.read(ctx, member, key)
	}
	record, found := client.stores[member.ID].GetRecord(key)
	return record, found, nil
}

func (client *replicaClient) Apply(ctx context.Context, member cluster.Member, record storage.Record) error {
	if client.write != nil {
		return client.write(ctx, member, record)
	}
	_, err := client.stores[member.ID].Apply(record)
	return err
}

func TestCoordinatorPlacesIdenticalRecordsOnDistinctReplicas(t *testing.T) {
	for _, factor := range []int{1, 3, 5} {
		t.Run(fmt.Sprint(factor), func(t *testing.T) {
			options, client := newOptions(t)
			options.ReplicationFactor = factor
			coordinator := newCoordinator(t, options)
			for _, key := range []string{"key", "other", ""} {
				if err := coordinator.Put(t.Context(), key, ""); err != nil {
					t.Fatal(err)
				}
				owners, err := options.Ring.GetNodes(key, factor)
				if err != nil {
					t.Fatal(err)
				}
				var expected storage.Record
				for id, store := range client.stores {
					record, found := store.GetRecord(key)
					if found != slices.Contains(owners, id) {
						t.Fatalf("unexpected placement on %s", id)
					}
					if !found {
						continue
					}
					if expected.Version.Counter == 0 {
						expected = record
					}
					if record != expected || record.Version.NodeID != options.LocalID {
						t.Fatalf("inconsistent replica: %+v", record)
					}
				}
				if value, found, err := coordinator.Get(t.Context(), key); err != nil || !found || value != "" {
					t.Fatalf("empty value: %q %v %v", value, found, err)
				}
				if err := coordinator.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
				for _, id := range owners {
					record, found := client.stores[id].GetRecord(key)
					if !found || !record.Deleted || record.Version.Compare(expected.Version) <= 0 {
						t.Fatalf("missing deletion on %s: %+v", id, record)
					}
				}
				if _, found, err := coordinator.Get(t.Context(), key); err != nil || found {
					t.Fatalf("deleted key: %v %v", found, err)
				}
			}
		})
	}
}

func TestCoordinatorResolvesVersionsBeforeReadingAndWriting(t *testing.T) {
	options, client := newOptions(t)
	coordinator := newCoordinator(t, options)
	owners, err := options.Ring.GetNodes("key", 3)
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range owners[:2] {
		record := storage.Record{Key: "key", Value: "older", Version: storage.Version{Counter: uint64(90 + index), NodeID: "remote"}}
		if index == 1 {
			record.Deleted = true
			record.Value = ""
		}
		if _, err := client.stores[id].Apply(record); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := coordinator.Get(t.Context(), "key"); found || err != nil {
		t.Fatalf("newest tombstone ignored: %v %v", found, err)
	}
	if err := coordinator.Put(t.Context(), "key", "new"); err != nil {
		t.Fatal(err)
	}
	for _, id := range owners {
		record, found := client.stores[id].GetRecord("key")
		if !found || record.Value != "new" || record.Version.Counter != 92 || record.Version.NodeID != options.LocalID {
			t.Fatalf("record on %s: %+v", id, record)
		}
	}
}

func TestCoordinatorRejectsConflictingReplicaVersions(t *testing.T) {
	options, client := newOptions(t)
	owners, err := options.Ring.GetNodes("key", 3)
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range owners {
		record := storage.Record{Key: "key", Value: fmt.Sprint(index), Version: storage.Version{Counter: 1, NodeID: "writer"}}
		if _, err := client.stores[id].Apply(record); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := newCoordinator(t, options)
	if _, _, err := coordinator.Get(t.Context(), "key"); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("Get: %v", err)
	}
	if err := coordinator.Put(t.Context(), "key", "new"); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("Put: %v", err)
	}
}

func TestCoordinatorRunsBothPhasesConcurrently(t *testing.T) {
	options, client := newOptions(t)
	key := remoteKey(t, options)
	reads, writes := make(chan string, 3), make(chan storage.Record, 3)
	releaseReads, releaseWrites := make(chan struct{}), make(chan struct{})
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		reads <- member.ID
		select {
		case <-releaseReads:
			return storage.Record{}, false, nil
		case <-ctx.Done():
			return storage.Record{}, false, ctx.Err()
		}
	}
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		writes <- record
		select {
		case <-releaseWrites:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	coordinator := newCoordinator(t, options)
	finished := make(chan error, 1)
	go func() { finished <- coordinator.Put(t.Context(), key, "value") }()
	for range 3 {
		select {
		case <-reads:
		case <-time.After(time.Second):
			t.Fatal("reads were not concurrent")
		}
	}
	close(releaseReads)
	var expected storage.Record
	for range 3 {
		select {
		case record := <-writes:
			if expected.Version.Counter == 0 {
				expected = record
			}
			if record != expected {
				t.Fatal("replicas received different records")
			}
		case <-time.After(time.Second):
			t.Fatal("writes were not concurrent")
		}
	}
	select {
	case err := <-finished:
		t.Fatalf("returned before acknowledgements: %v", err)
	default:
	}
	close(releaseWrites)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorReadFailurePreventsWrites(t *testing.T) {
	options, client := newOptions(t)
	key := remoteKey(t, options)
	client.read = func(context.Context, cluster.Member, string) (storage.Record, bool, error) {
		return storage.Record{}, false, replication.ErrUnavailable
	}
	client.write = func(context.Context, cluster.Member, storage.Record) error {
		t.Error("write attempted after failed observation")
		return nil
	}
	coordinator := newCoordinator(t, options)
	if err := coordinator.Put(t.Context(), key, "value"); !errors.Is(err, replication.ErrUnavailable) {
		t.Fatalf("Put: %v", err)
	}
}

func TestCoordinatorReportsPartialWriteWithoutRollback(t *testing.T) {
	options, client := newOptions(t)
	key := remoteKey(t, options)
	owners, err := options.Ring.GetNodes(key, 3)
	if err != nil {
		t.Fatal(err)
	}
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		if member.ID == owners[0] {
			return replication.ErrUnavailable
		}
		_, err := client.stores[member.ID].Apply(record)
		return err
	}
	if err := newCoordinator(t, options).Put(t.Context(), key, "value"); !errors.Is(err, replication.ErrUnavailable) {
		t.Fatalf("Put: %v", err)
	}
	for index, id := range owners {
		if _, found := client.stores[id].GetRecord(key); found != (index != 0) {
			t.Fatalf("unexpected partial write on %s", id)
		}
	}
}

func TestCoordinatorDoesNotAcknowledgeStaleReplicaWrite(t *testing.T) {
	options, client := newOptions(t)
	key := remoteKey(t, options)
	client.write = func(ctx context.Context, member cluster.Member, record storage.Record) error {
		newer := record
		newer.Version.Counter++
		if _, err := client.stores[member.ID].Apply(newer); err != nil {
			return err
		}
		_, err := client.stores[member.ID].Apply(record)
		return err
	}
	if err := newCoordinator(t, options).Put(t.Context(), key, "value"); !errors.Is(err, storage.ErrStaleRecord) {
		t.Fatalf("Put: %v", err)
	}
}

func TestCoordinatorCancellationStopsReplicaCalls(t *testing.T) {
	for _, phase := range []string{"read", "write"} {
		t.Run(phase, func(t *testing.T) {
			options, client := newOptions(t)
			key := remoteKey(t, options)
			started, exited := make(chan struct{}, 3), make(chan struct{}, 3)
			block := func(ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				exited <- struct{}{}
				return ctx.Err()
			}
			if phase == "read" {
				client.read = func(ctx context.Context, _ cluster.Member, _ string) (storage.Record, bool, error) {
					return storage.Record{}, false, block(ctx)
				}
			}
			if phase == "write" {
				client.write = func(ctx context.Context, _ cluster.Member, _ storage.Record) error { return block(ctx) }
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			coordinator := newCoordinator(t, options)
			go func() { finished <- coordinator.Put(ctx, key, "value") }()
			for range 3 {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("replica call did not start")
				}
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Put: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not return")
			}
			for range 3 {
				select {
				case <-exited:
				case <-time.After(time.Second):
					t.Fatal("replica worker did not exit")
				}
			}
		})
	}
}

func TestCoordinatorConcurrentWritersReserveDistinctVersions(t *testing.T) {
	options, client := newOptions(t)
	coordinator := newCoordinator(t, options)
	var pending sync.WaitGroup
	for index := range 32 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			if err := coordinator.Put(t.Context(), fmt.Sprint(index), "value"); err != nil {
				t.Error(err)
			}
		}()
	}
	pending.Wait()
	seen := make(map[storage.Version]bool)
	for index := range 32 {
		key := fmt.Sprint(index)
		owners, err := options.Ring.GetNodes(key, 3)
		if err != nil {
			t.Fatal(err)
		}
		record, found := client.stores[owners[0]].GetRecord(key)
		if !found || seen[record.Version] {
			t.Fatal("missing record or reused version")
		}
		seen[record.Version] = true
	}
}

func TestConcurrentCoordinatorsResolveCounterTiesByNodeID(t *testing.T) {
	options, client := newOptions(t)
	key := remoteKey(t, options)
	owners, err := options.Ring.GetNodes(key, 3)
	if err != nil {
		t.Fatal(err)
	}
	var writers []string
	for _, id := range options.Membership.NodeIDs() {
		if !slices.Contains(owners, id) {
			writers = append(writers, id)
		}
	}
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	client.read = func(ctx context.Context, member cluster.Member, key string) (storage.Record, bool, error) {
		record, found := client.stores[member.ID].GetRecord(key)
		started <- struct{}{}
		select {
		case <-release:
			return record, found, nil
		case <-ctx.Done():
			return storage.Record{}, false, ctx.Err()
		}
	}
	finished := make(chan error, 2)
	for _, id := range writers {
		options.LocalID, options.Store = id, client.stores[id]
		coordinator := newCoordinator(t, options)
		go func() { finished <- coordinator.Put(t.Context(), key, id) }()
	}
	for range 6 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("coordinators did not observe replicas concurrently")
		}
	}
	close(release)
	successes := 0
	for range 2 {
		err := <-finished
		if err == nil {
			successes++
		} else if !errors.Is(err, storage.ErrStaleRecord) {
			t.Fatal(err)
		}
	}
	if successes == 0 {
		t.Fatal("neither writer succeeded")
	}
	for _, id := range owners {
		record, found := client.stores[id].GetRecord(key)
		if !found || record.Version.Counter != 1 || record.Version.NodeID != writers[1] || record.Value != writers[1] {
			t.Fatalf("tie resolved incorrectly on %s: %+v", id, record)
		}
	}
}

func TestCoordinatorDeadlineBoundsTheWholeOperation(t *testing.T) {
	options, client := newOptions(t)
	options.Timeout = 50 * time.Millisecond
	key := remoteKey(t, options)
	client.read = func(ctx context.Context, _ cluster.Member, _ string) (storage.Record, bool, error) {
		<-ctx.Done()
		return storage.Record{}, false, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := newCoordinator(t, options).Put(ctx, key, "value"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Put: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("coordinator relied on the caller's longer deadline")
	}
}

func TestNewCoordinatorRejectsInvalidOptions(t *testing.T) {
	cases := map[string]func(*replication.Options){
		"store": func(o *replication.Options) { o.Store = nil }, "client": func(o *replication.Options) { o.Client = nil },
		"membership": func(o *replication.Options) { o.Membership = nil }, "ring": func(o *replication.Options) { o.Ring = nil },
		"local ID": func(o *replication.Options) { o.LocalID = "unknown" }, "zero replicas": func(o *replication.Options) { o.ReplicationFactor = 0 },
		"too many replicas": func(o *replication.Options) { o.ReplicationFactor = 6 }, "timeout": func(o *replication.Options) { o.Timeout = 0 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			options, _ := newOptions(t)
			change(&options)
			if _, err := replication.NewCoordinator(options); err == nil {
				t.Fatal("accepted invalid options")
			}
		})
	}
}

func newOptions(t *testing.T) (replication.Options, *replicaClient) {
	t.Helper()
	client := &replicaClient{stores: make(map[string]*storage.MemoryStore)}
	var members []cluster.Member
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		store, err := storage.NewMemoryStore(id)
		if err != nil {
			t.Fatal(err)
		}
		client.stores[id] = store
		members = append(members, cluster.Member{ID: id, Address: "http://" + id + ":8001"})
	}
	membership, err := cluster.NewMembership(members)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := consistenthash.NewRing(membership.NodeIDs(), 32)
	if err != nil {
		t.Fatal(err)
	}
	return replication.Options{LocalID: "e", Store: client.stores["e"], Client: client, Membership: membership, Ring: ring, ReplicationFactor: 3, Timeout: 5 * time.Second}, client
}

func newCoordinator(t *testing.T, options replication.Options) *replication.Coordinator {
	t.Helper()
	coordinator, err := replication.NewCoordinator(options)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func remoteKey(t *testing.T, options replication.Options) string {
	t.Helper()
	for index := range 1000 {
		key := fmt.Sprint(index)
		owners, err := options.Ring.GetNodes(key, 3)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(owners, options.LocalID) {
			return key
		}
	}
	t.Fatal("no key outside local replica set")
	return ""
}
