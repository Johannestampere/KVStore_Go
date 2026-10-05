package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

// ErrStoreClosed indicates that storage no longer accepts mutations.
var ErrStoreClosed = errors.New("store is closed")

// PersistentStore journals mutations before publishing them to memory.
// Reads remain available after Close. Construct it with OpenPersistentStore
// or NewPersistentStore; do not copy it after use.
type PersistentStore struct {
	memory     *MemoryStore
	journal    Journal
	commands   chan func()
	stopping   chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
	closeError error
	writeError error
}

// OpenPersistentStore restores an exclusively locked node-local directory.
func OpenPersistentStore(directory, nodeID string) (*PersistentStore, error) {
	if err := validateNodeID(nodeID); err != nil {
		return nil, err
	}
	journal, err := openFileLog(directory, nodeID)
	if err != nil {
		return nil, fmt.Errorf("open persistent store: %w", err)
	}
	return NewPersistentStore(nodeID, journal)
}

// NewPersistentStore takes ownership of journal, replays it, and starts a writer.
// It closes journal if recovery fails.
func NewPersistentStore(nodeID string, journal Journal) (*PersistentStore, error) {
	if journal == nil {
		return nil, errors.New("persistent store requires a journal")
	}
	memory, err := NewMemoryStore(nodeID)
	if err != nil {
		return nil, errors.Join(err, journal.Close())
	}
	store := &PersistentStore{
		memory: memory, journal: journal, commands: make(chan func()),
		stopping: make(chan struct{}), done: make(chan struct{}),
	}
	if err := journal.Replay(store.restore); err != nil {
		return nil, errors.Join(fmt.Errorf("replay storage: %w", err), journal.Close())
	}
	go store.runWriter()
	return store, nil
}

// Get returns the last committed live value, or false for a missing key.
func (store *PersistentStore) Get(key string) (string, bool) {
	return store.memory.Get(key)
}

// GetRecord returns the last committed record, including deletion markers.
func (store *PersistentStore) GetRecord(key string) (Record, bool) {
	return store.memory.GetRecord(key)
}

// Put durably replaces a value using a new local version.
func (store *PersistentStore) Put(key, value string) error {
	return store.writeLocal(Record{Key: key, Value: value})
}

// Delete durably records a deletion, including for an absent key.
func (store *PersistentStore) Delete(key string) error {
	return store.writeLocal(Record{Key: key, Deleted: true})
}

func (store *PersistentStore) writeLocal(record Record) error {
	return store.submit(func() error {
		version, err := store.nextVersion(Version{})
		if err != nil {
			return err
		}
		record.Version = version
		return store.commit(LogEntry{Counter: version.Counter, Record: &record})
	})
}

// Apply durably accepts a newer record; identical retries return false, nil.
func (store *PersistentStore) Apply(record Record) (bool, error) {
	var applied bool
	err := store.submit(func() error {
		current, found := store.memory.GetRecord(record.Key)
		update, err := shouldApply(record, current, found)
		if err != nil || !update {
			return err
		}
		entry := LogEntry{Counter: max(store.memory.counter, record.Version.Counter), Record: &record}
		if err := store.commit(entry); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

// NextVersion durably reserves a version without storing a replica.
func (store *PersistentStore) NextVersion(observed Version) (Version, error) {
	var version Version
	err := store.submit(func() error {
		next, err := store.nextVersion(observed)
		if err != nil {
			return err
		}
		if err := store.commit(LogEntry{Counter: next.Counter}); err != nil {
			return err
		}
		version = next
		return nil
	})
	return version, err
}

func (store *PersistentStore) nextVersion(observed Version) (Version, error) {
	counter := max(store.memory.counter, observed.Counter)
	if counter == math.MaxUint64 {
		return Version{}, ErrVersionExhausted
	}
	return Version{Counter: counter + 1, NodeID: store.memory.nodeID}, nil
}

func (store *PersistentStore) commit(entry LogEntry) error {
	if err := store.journal.Append(entry); err != nil {
		// A failed sync has an uncertain outcome; never append past it.
		store.writeError = fmt.Errorf("journal failed; reopen storage before writing: %w", err)
		return store.writeError
	}
	store.publish(entry)
	return nil
}

func (store *PersistentStore) publish(entry LogEntry) {
	store.memory.mu.Lock()
	defer store.memory.mu.Unlock()
	store.memory.counter = entry.Counter
	if entry.Record != nil {
		store.memory.save(*entry.Record)
	}
}

func (store *PersistentStore) restore(entry LogEntry) error {
	if entry.Counter == 0 || entry.Counter < store.memory.counter {
		return errors.New("log counter is zero or decreases")
	}
	if entry.Record == nil {
		if entry.Counter == store.memory.counter {
			return errors.New("log reservation does not advance the counter")
		}
	} else {
		record := *entry.Record
		current, found := store.memory.GetRecord(record.Key)
		update, err := shouldApply(record, current, found)
		if err != nil {
			return err
		}
		if !update || entry.Counter < record.Version.Counter {
			return errors.New("log record is duplicated or exceeds its counter")
		}
	}
	store.publish(entry)
	return nil
}

func (store *PersistentStore) submit(operation func() error) error {
	result := make(chan error, 1)
	select {
	case <-store.stopping:
		return ErrStoreClosed
	case store.commands <- func() {
		select {
		case <-store.stopping:
			result <- ErrStoreClosed
			return
		default:
		}
		if store.writeError != nil {
			result <- store.writeError
			return
		}
		result <- operation()
	}:
		return <-result
	}
}

func (store *PersistentStore) runWriter() {
	defer close(store.done)
	for {
		select {
		case <-store.stopping:
			store.closeError = store.journal.Close()
			return
		case command := <-store.commands:
			command()
		}
	}
}

// Close finishes an admitted mutation and releases the journal. It is idempotent.
func (store *PersistentStore) Close() error {
	store.closeOnce.Do(func() { close(store.stopping) })
	<-store.done
	return store.closeError
}
