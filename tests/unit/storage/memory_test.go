package storage_test

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"kvstore/internal/storage"
)

func TestMemoryStorePutAndGet(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "ordinary value", key: "user:1", value: "Alice"},
		{name: "empty value", key: "user:1", value: ""},
		{name: "empty key", key: "", value: "Alice"},
		{name: "unicode", key: "名前", value: "アリス"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Put(testCase.key, testCase.value); err != nil {
				t.Error(err)
				return
			}

			value, found := store.Get(testCase.key)
			if !found || value != testCase.value {
				t.Fatalf("Get(%q) = (%q, %t), want (%q, true)", testCase.key, value, found, testCase.value)
			}
		})
	}
}

func TestMemoryStoreMissingKey(t *testing.T) {
	store := newTestStore(t)

	value, found := store.Get("missing")
	if found || value != "" {
		t.Fatalf("Get(missing) = (%q, %t), want (\"\", false)", value, found)
	}
}

func TestMemoryStoreOverwritePreservesOtherKeys(t *testing.T) {
	store := newTestStore(t)
	if err := store.Put("user:1", "Alice"); err != nil {
		t.Error(err)
		return
	}
	if err := store.Put("user:2", "Bob"); err != nil {
		t.Error(err)
		return
	}
	if err := store.Put("user:1", "Carol"); err != nil {
		t.Error(err)
		return
	}

	for key, expected := range map[string]string{"user:1": "Carol", "user:2": "Bob"} {
		value, found := store.Get(key)
		if !found || value != expected {
			t.Errorf("Get(%q) = (%q, %t), want (%q, true)", key, value, found, expected)
		}
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	store := newTestStore(t)
	if err := store.Put("user:1", "Alice"); err != nil {
		t.Error(err)
		return
	}
	if err := store.Put("user:2", "Bob"); err != nil {
		t.Error(err)
		return
	}

	if err := store.Delete("user:1"); err != nil {
		t.Error(err)
		return
	}
	if err := store.Delete("user:1"); err != nil {
		t.Error(err)
		return
	}
	if err := store.Delete("missing"); err != nil {
		t.Error(err)
		return
	}

	if value, found := store.Get("user:1"); found || value != "" {
		t.Errorf("Get(deleted key) = (%q, %t), want (\"\", false)", value, found)
	}
	if value, found := store.Get("user:2"); !found || value != "Bob" {
		t.Errorf("Get(other key) = (%q, %t), want (\"Bob\", true)", value, found)
	}

	if err := store.Put("user:1", "Carol"); err != nil {
		t.Error(err)
		return
	}
	if value, found := store.Get("user:1"); !found || value != "Carol" {
		t.Errorf("Get(reinserted key) = (%q, %t), want (\"Carol\", true)", value, found)
	}
}

func TestMemoryStoreZeroValue(t *testing.T) {
	var store storage.MemoryStore
	if err := store.Delete("missing"); err == nil {
		t.Fatal("zero-value store accepted a deletion without a node ID")
	}
	if value, found := store.Get("missing"); found || value != "" {
		t.Fatal("zero-value store returned a value")
	}
	if err := store.Put("user:1", "Alice"); err == nil {
		t.Fatal("zero-value store accepted a write without a node ID")
	}
	if _, found := store.GetRecord("user:1"); found {
		t.Fatal("rejected write changed storage")
	}
}

func TestMemoryStoreConcurrentOperations(t *testing.T) {
	store := newTestStore(t)
	const workers = 16
	const iterations = 100
	var pending sync.WaitGroup
	start := make(chan struct{})

	for worker := 0; worker < workers; worker++ {
		pending.Add(1)
		go func(worker int) {
			defer pending.Done()
			<-start
			key := fmt.Sprintf("worker:%d", worker)

			for iteration := 0; iteration < iterations; iteration++ {
				expected := fmt.Sprintf("value:%d", iteration)
				if err := store.Put(key, expected); err != nil {
					t.Error(err)
					return
				}
				if value, found := store.Get(key); !found || value != expected {
					t.Errorf("Get(%q) = (%q, %t), want (%q, true)", key, value, found, expected)
					return
				}
				if err := store.Delete(key); err != nil {
					t.Error(err)
					return
				}
				if value, found := store.Get(key); found || value != "" {
					t.Errorf("Get(%q) after deletion = (%q, %t), want (\"\", false)", key, value, found)
					return
				}

				// Other workers may delete this key between Put and Get.
				if err := store.Put("shared", "shared value"); err != nil {
					t.Error(err)
					return
				}
				value, found := store.Get("shared")
				if (found && value != "shared value") || (!found && value != "") {
					t.Errorf("Get(shared) returned an invalid pair: (%q, %t)", value, found)
					return
				}
				if err := store.Delete("shared"); err != nil {
					t.Error(err)
					return
				}
			}
			if err := store.Put(key, "finished"); err != nil {
				t.Error(err)
				return
			}
		}(worker)
	}

	close(start)
	pending.Wait()

	for worker := 0; worker < workers; worker++ {
		key := fmt.Sprintf("worker:%d", worker)
		if value, found := store.Get(key); !found || value != "finished" {
			t.Errorf("Get(%q) after workers finish = (%q, %t), want (\"finished\", true)", key, value, found)
		}
	}
}

func TestMemoryStoreApplyOrdering(t *testing.T) {
	current := storage.Record{Key: "key", Value: "current", Version: storage.Version{Counter: 10, NodeID: "b"}}
	cases := []struct {
		name     string
		incoming storage.Record
		applied  bool
		err      error
	}{
		{"older counter", storage.Record{Key: "key", Value: "old", Version: storage.Version{Counter: 9, NodeID: "z"}}, false, storage.ErrStaleRecord},
		{"lower node ID", storage.Record{Key: "key", Value: "old", Version: storage.Version{Counter: 10, NodeID: "a"}}, false, storage.ErrStaleRecord},
		{"higher node ID", storage.Record{Key: "key", Value: "winner", Version: storage.Version{Counter: 10, NodeID: "c"}}, true, nil},
		{"newer counter", storage.Record{Key: "key", Value: "new", Version: storage.Version{Counter: 11, NodeID: "a"}}, true, nil},
		{"identical retry", current, false, nil},
		{"conflicting value", storage.Record{Key: "key", Value: "different", Version: current.Version}, false, storage.ErrVersionConflict},
		{"conflicting deletion", storage.Record{Key: "key", Deleted: true, Version: current.Version}, false, storage.ErrVersionConflict},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := newTestStore(t)
			if applied, err := store.Apply(current); err != nil || !applied {
				t.Fatalf("initial Apply = (%v, %v)", applied, err)
			}
			applied, err := store.Apply(testCase.incoming)
			if applied != testCase.applied || !errors.Is(err, testCase.err) {
				t.Fatalf("Apply = (%v, %v), want (%v, %v)", applied, err, testCase.applied, testCase.err)
			}
			expected := current
			if testCase.applied {
				expected = testCase.incoming
			}
			if actual, found := store.GetRecord("key"); !found || actual != expected {
				t.Fatalf("record = %+v, want %+v", actual, expected)
			}
		})
	}
}

func TestMemoryStoreDeletionRejectsStaleRecordsInEitherOrder(t *testing.T) {
	live := storage.Record{Key: "key", Value: "old", Version: storage.Version{Counter: 1, NodeID: "a"}}
	deleted := storage.Record{Key: "key", Deleted: true, Version: storage.Version{Counter: 2, NodeID: "b"}}
	for _, records := range [][]storage.Record{{live, deleted}, {deleted, live}} {
		store := newTestStore(t)
		for _, record := range records {
			if _, err := store.Apply(record); err != nil && !errors.Is(err, storage.ErrStaleRecord) {
				t.Fatal(err)
			}
		}
		if _, found := store.Get("key"); found {
			t.Fatal("stale value resurrected deleted key")
		}
		if record, found := store.GetRecord("key"); !found || record != deleted {
			t.Fatalf("deletion marker lost: %+v", record)
		}
		if applied, err := store.Apply(deleted); applied || err != nil {
			t.Fatalf("repeated deletion Apply = (%v, %v)", applied, err)
		}
		if err := store.Put("key", "new"); err != nil {
			t.Fatal(err)
		}
		if record, found := store.GetRecord("key"); !found || record.Deleted || record.Value != "new" || record.Version.Counter != 3 {
			t.Fatalf("new write did not supersede deletion: %+v", record)
		}
	}
}

func TestMemoryStoreDeleteAbsentKeyCreatesMarker(t *testing.T) {
	store := newTestStore(t)
	for expected := uint64(1); expected <= 2; expected++ {
		if err := store.Delete("missing"); err != nil {
			t.Fatal(err)
		}
		record, found := store.GetRecord("missing")
		if !found || !record.Deleted || record.Value != "" || record.Version != (storage.Version{Counter: expected, NodeID: "node-a"}) {
			t.Fatalf("deletion marker = %+v", record)
		}
	}
}

func TestMemoryStoreAdvancesBeyondObservedVersionsAcrossKeys(t *testing.T) {
	store := newTestStore(t)
	remote := storage.Record{Key: "remote", Value: "value", Version: storage.Version{Counter: 100, NodeID: "node-z"}}
	if _, err := store.Apply(remote); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("local", "value"); err != nil {
		t.Fatal(err)
	}
	record, found := store.GetRecord("local")
	if !found || record.Version != (storage.Version{Counter: 101, NodeID: "node-a"}) {
		t.Fatalf("local version = %+v", record.Version)
	}
	if err := store.Delete("remote"); err != nil {
		t.Fatal(err)
	}
	record, found = store.GetRecord("remote")
	if !found || !record.Deleted || record.Version.Counter != 102 {
		t.Fatalf("delete after observation = %+v", record)
	}
}

func TestMemoryStoreRejectsInvalidRecordsWithoutAdvancingCounter(t *testing.T) {
	cases := []storage.Record{
		{Key: "invalid", Version: storage.Version{NodeID: "a"}},
		{Key: "invalid", Version: storage.Version{Counter: 100}},
		{Key: "invalid", Value: "value", Deleted: true, Version: storage.Version{Counter: 100, NodeID: "a"}},
	}
	for _, record := range cases {
		store := newTestStore(t)
		if applied, err := store.Apply(record); applied || err == nil {
			t.Fatalf("accepted invalid record: %+v", record)
		}
		if _, found := store.GetRecord("invalid"); found {
			t.Fatal("invalid record stored")
		}
		if err := store.Put("valid", "value"); err != nil {
			t.Fatal(err)
		}
		if actual, found := store.GetRecord("valid"); !found || actual.Version.Counter != 1 {
			t.Fatal("invalid record advanced counter")
		}
	}
}

func TestMemoryStoreCounterExhaustionDoesNotChangeRecords(t *testing.T) {
	store := newTestStore(t)
	record := storage.Record{Key: "key", Value: "last", Version: storage.Version{Counter: math.MaxUint64, NodeID: "z"}}
	if _, err := store.Apply(record); err != nil && !errors.Is(err, storage.ErrStaleRecord) {
		t.Fatal(err)
	}
	if err := store.Put("key", "wrapped"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Put error = %v", err)
	}
	if err := store.Delete("key"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("Delete error = %v", err)
	}
	if actual, found := store.GetRecord("key"); !found || actual != record {
		t.Fatal("exhaustion changed record")
	}
}

func TestMemoryStoreRecordCopiesDoNotShareMutableState(t *testing.T) {
	store := newTestStore(t)
	original := storage.Record{Key: "key", Value: "original", Version: storage.Version{Counter: 1, NodeID: "a"}}
	incoming := original
	if _, err := store.Apply(incoming); err != nil {
		t.Fatal(err)
	}
	incoming.Value = "changed"
	returned, found := store.GetRecord("key")
	if !found {
		t.Fatal("record missing")
	}
	returned.Value = "changed again"
	returned.Version.Counter = 999
	if actual, found := store.GetRecord("key"); !found || actual != original {
		t.Fatal("caller mutation changed stored record")
	}
}

func TestMemoryStoreConcurrentApplyKeepsHighestVersion(t *testing.T) {
	store := newTestStore(t)
	var pending sync.WaitGroup
	start := make(chan struct{})
	for counter := uint64(1); counter <= 100; counter++ {
		pending.Add(1)
		go func() {
			defer pending.Done()
			<-start
			record := storage.Record{Key: "key", Value: fmt.Sprint(counter), Version: storage.Version{Counter: counter, NodeID: "remote"}}
			if _, err := store.Apply(record); err != nil && !errors.Is(err, storage.ErrStaleRecord) {
				t.Error(err)
				return
			}
			actual, found := store.GetRecord("key")
			if !found || actual.Value != fmt.Sprint(actual.Version.Counter) {
				t.Errorf("inconsistent record: %+v", actual)
			}
		}()
	}
	close(start)
	pending.Wait()
	if record, found := store.GetRecord("key"); !found || record.Version.Counter != 100 {
		t.Fatalf("highest version lost: %+v", record)
	}
}

func TestMemoryStoreConcurrentLocalUpdatesHaveUniqueVersions(t *testing.T) {
	store := newTestStore(t)
	var pending sync.WaitGroup
	for index := 0; index < 100; index++ {
		pending.Add(1)
		go func() {
			defer pending.Done()
			if err := store.Put(fmt.Sprint(index), "value"); err != nil {
				t.Error(err)
			}
		}()
	}
	pending.Wait()
	seen := make(map[storage.Version]bool)
	for index := 0; index < 100; index++ {
		record, found := store.GetRecord(fmt.Sprint(index))
		if !found || record.Version.Counter == 0 || record.Version.Counter > 100 || record.Version.NodeID != "node-a" || seen[record.Version] {
			t.Fatalf("invalid or reused version: %+v", record.Version)
		}
		seen[record.Version] = true
	}
}

func TestNextVersionReservesWithoutWritingARecord(t *testing.T) {
	store := newTestStore(t)
	first, err := store.NextVersion(storage.Version{Counter: 100, NodeID: "remote"})
	if err != nil {
		t.Fatal(err)
	}
	if first != (storage.Version{Counter: 101, NodeID: "node-a"}) {
		t.Fatalf("reserved version = %+v", first)
	}
	if _, found := store.GetRecord(""); found {
		t.Fatal("version reservation created a record")
	}
	second, err := store.NextVersion(storage.Version{Counter: 1, NodeID: "remote"})
	if err != nil || second.Counter != 102 {
		t.Fatalf("second version = %+v, %v", second, err)
	}
	if err := store.Put("key", "value"); err != nil {
		t.Fatal(err)
	}
	record, found := store.GetRecord("key")
	if !found || record.Version.Counter != 103 {
		t.Fatalf("local write reused version: %+v", record)
	}
	if _, err := store.NextVersion(storage.Version{Counter: math.MaxUint64, NodeID: "remote"}); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("overflow = %v", err)
	}
	if _, err := store.NextVersion(storage.Version{}); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("counter wrapped: %v", err)
	}
}

func ExampleMemoryStore() {
	store, err := storage.NewMemoryStore("node-a")
	if err != nil {
		panic(err)
	}
	if err := store.Put("user:1", "Alice"); err != nil {
		panic(err)
	}
	value, found := store.Get("user:1")
	fmt.Println(value, found)
	if err := store.Delete("user:1"); err != nil {
		panic(err)
	}
	_, found = store.Get("user:1")
	fmt.Println(found)
	// Output:
	// Alice true
	// false
}

func newTestStore(t *testing.T) *storage.MemoryStore {
	t.Helper()
	store, err := storage.NewMemoryStore("node-a")
	if err != nil {
		t.Fatal(err)
	}
	return store
}
