package storage_test

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"kvstore/internal/storage"
)

type journalStub struct {
	entries          []storage.LogEntry
	appendError      error
	replayError      error
	closeError       error
	keepFailedAppend bool
	appendCalls      int
	closeCalls       int
	started          chan struct{}
	release          chan struct{}
}

func (journal *journalStub) Replay(visit func(storage.LogEntry) error) error {
	if journal.replayError != nil {
		return journal.replayError
	}
	for _, entry := range journal.entries {
		if err := visit(entry); err != nil {
			return err
		}
	}
	return nil
}

func (journal *journalStub) Append(entry storage.LogEntry) error {
	journal.appendCalls++
	if journal.started != nil {
		close(journal.started)
		<-journal.release
	}
	if journal.appendError == nil || journal.keepFailedAppend {
		journal.entries = append(journal.entries, entry)
	}
	return journal.appendError
}

func (journal *journalStub) Close() error {
	journal.closeCalls++
	return journal.closeError
}

func newJournalStore(t *testing.T, journal *journalStub) *storage.PersistentStore {
	t.Helper()
	store, err := storage.NewPersistentStore("node-a", journal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestPersistentStorePublishesOnlyAfterJournalAcknowledgement(t *testing.T) {
	journal := &journalStub{started: make(chan struct{}), release: make(chan struct{})}
	store := newJournalStore(t, journal)
	// Release before cleanup even if an assertion fails while the writer is blocked.
	var release sync.Once
	defer release.Do(func() { close(journal.release) })
	written := make(chan error, 1)
	go func() { written <- store.Put("key", "value") }()
	select {
	case <-journal.started:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not reach journal")
	}
	read := make(chan bool, 1)
	go func() { _, found := store.Get("key"); read <- found }()
	select {
	case found := <-read:
		if found {
			t.Fatal("uncommitted value is visible")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disk append blocked an independent read")
	}
	select {
	case err := <-written:
		t.Fatalf("write returned before synchronization: %v", err)
	default:
	}
	release.Do(func() { close(journal.release) })
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if value, found := store.Get("key"); !found || value != "value" {
		t.Fatalf("committed value: %q, %t", value, found)
	}
}

func TestPersistentStoreStopsWritingAfterUncertainAppend(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("entry_survives_%t", keep), func(t *testing.T) {
			failure := errors.New("disk synchronization failed")
			journal := &journalStub{appendError: failure, keepFailedAppend: keep}
			store := newJournalStore(t, journal)
			if err := store.Put("key", "uncertain"); !errors.Is(err, failure) {
				t.Fatalf("Put: %v", err)
			}
			if _, found := store.Get("key"); found {
				t.Fatal("failed write was published")
			}
			if err := store.Delete("key"); !errors.Is(err, failure) {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := store.NextVersion(storage.Version{}); !errors.Is(err, failure) {
				t.Fatalf("NextVersion: %v", err)
			}
			if journal.appendCalls != 1 {
				t.Fatalf("appended after failure: %d calls", journal.appendCalls)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			recovered := newJournalStore(t, &journalStub{entries: journal.entries})
			if _, found := recovered.Get("key"); found != keep {
				t.Fatalf("recovery found=%t, want %t", found, keep)
			}
		})
	}
}

func TestPersistentStoreSharesRecordConflictRules(t *testing.T) {
	journal := &journalStub{}
	store := newJournalStore(t, journal)
	record := storage.Record{Key: "key", Value: "latest", Version: storage.Version{Counter: 7, NodeID: "node-b"}}
	if applied, err := store.Apply(record); !applied || err != nil {
		t.Fatalf("Apply: %t, %v", applied, err)
	}
	if applied, err := store.Apply(record); applied || err != nil {
		t.Fatalf("retry: %t, %v", applied, err)
	}
	conflict := record
	conflict.Value = "other"
	if _, err := store.Apply(conflict); !errors.Is(err, storage.ErrVersionConflict) {
		t.Fatalf("conflict: %v", err)
	}
	stale := record
	stale.Version.Counter--
	if _, err := store.Apply(stale); !errors.Is(err, storage.ErrStaleRecord) {
		t.Fatalf("stale: %v", err)
	}
	invalid := record
	invalid.Version.Counter = 0
	if _, err := store.Apply(invalid); err == nil {
		t.Fatal("accepted zero version")
	}
	if journal.appendCalls != 1 {
		t.Fatalf("unchanged records reached journal: %d", journal.appendCalls)
	}
	version, err := store.NextVersion(storage.Version{Counter: 40})
	if err != nil || version.Counter != 41 {
		t.Fatalf("version: %+v, %v", version, err)
	}
	if err := store.Delete("key"); err != nil {
		t.Fatal(err)
	}
	deleted, found := store.GetRecord("key")
	if !found || !deleted.Deleted || deleted.Version.Counter != 42 {
		t.Fatalf("tombstone: %+v", deleted)
	}
	if _, found := store.Get("key"); found {
		t.Fatal("tombstone returned as value")
	}
}

func TestPersistentStoreRestoresReservationsAndChecksOverflow(t *testing.T) {
	journal := &journalStub{entries: []storage.LogEntry{{Counter: math.MaxUint64 - 1}}}
	store := newJournalStore(t, journal)
	version, err := store.NextVersion(storage.Version{})
	if err != nil || version.Counter != math.MaxUint64 {
		t.Fatalf("version: %+v, %v", version, err)
	}
	if err := store.Put("key", "value"); !errors.Is(err, storage.ErrVersionExhausted) {
		t.Fatalf("overflow: %v", err)
	}
	if journal.appendCalls != 1 {
		t.Fatalf("overflow reached journal: %d", journal.appendCalls)
	}
}

func TestPersistentStoreRejectsInvalidReplayAndClosesJournal(t *testing.T) {
	record := storage.Record{Key: "key", Version: storage.Version{Counter: 5, NodeID: "node-b"}}
	cases := map[string][]storage.LogEntry{
		"zero":                  {{Counter: 0}},
		"decreasing":            {{Counter: 9}, {Counter: 8}},
		"repeated reservation":  {{Counter: 9}, {Counter: 9}},
		"record beyond counter": {{Counter: 4, Record: &record}},
		"duplicate record":      {{Counter: 5, Record: &record}, {Counter: 5, Record: &record}},
		"invalid record":        {{Counter: 1, Record: &storage.Record{}}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			journal := &journalStub{entries: entries}
			store, err := storage.NewPersistentStore("node-a", journal)
			if err == nil {
				store.Close()
				t.Fatal("accepted invalid log")
			}
			if journal.closeCalls != 1 {
				t.Fatalf("journal closed %d times", journal.closeCalls)
			}
		})
	}
}

func TestPersistentStoreConcurrentMutationsAndClose(t *testing.T) {
	journal := &journalStub{}
	store := newJournalStore(t, journal)
	var workers sync.WaitGroup
	for worker := range 30 {
		workers.Go(func() {
			key := fmt.Sprintf("key-%d", worker)
			if err := store.Put(key, key); err != nil {
				t.Error(err)
				return
			}
			if value, found := store.Get(key); !found || value != key {
				t.Errorf("Get(%q)=%q,%t", key, value, found)
			}
		})
	}
	workers.Wait()
	seen := make(map[uint64]bool)
	for _, entry := range journal.entries {
		if seen[entry.Counter] {
			t.Fatalf("reused counter %d", entry.Counter)
		}
		seen[entry.Counter] = true
	}
	for range 10 {
		workers.Go(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if journal.closeCalls != 1 {
		t.Fatalf("closed journal %d times", journal.closeCalls)
	}
	if err := store.Put("closed", "value"); !errors.Is(err, storage.ErrStoreClosed) {
		t.Fatalf("closed Put: %v", err)
	}
}

func TestPersistentStorePropagatesReplayAndCloseErrors(t *testing.T) {
	replayFailure := errors.New("read failed")
	closeFailure := errors.New("close failed")
	journal := &journalStub{replayError: replayFailure, closeError: closeFailure}
	if _, err := storage.NewPersistentStore("node-a", journal); !errors.Is(err, replayFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("recovery errors: %v", err)
	}
	store, err := storage.NewPersistentStore("node-a", &journalStub{closeError: closeFailure})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("Close: %v", err)
	}
}

func TestPersistentStoreCloseWaitsForAdmittedWrite(t *testing.T) {
	journal := &journalStub{started: make(chan struct{}), release: make(chan struct{})}
	store := newJournalStore(t, journal)
	var release sync.Once
	defer release.Do(func() { close(journal.release) })
	written := make(chan error, 1)
	go func() { written <- store.Put("key", "value") }()
	select {
	case <-journal.started:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	rejected := make(chan error, 1)
	go func() { _, err := store.NextVersion(storage.Version{}); rejected <- err }()
	select {
	case err := <-rejected:
		if !errors.Is(err, storage.ErrStoreClosed) {
			t.Fatalf("new mutation during close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not stop admission")
	}
	select {
	case err := <-closed:
		t.Fatalf("closed while append was pending: %v", err)
	default:
	}
	release.Do(func() { close(journal.release) })
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish")
	}
	if value, found := store.Get("key"); !found || value != "value" {
		t.Fatal("admitted write was lost")
	}
	if journal.closeCalls != 1 {
		t.Fatalf("journal closed %d times", journal.closeCalls)
	}
}
