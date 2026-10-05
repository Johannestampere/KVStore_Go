package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

// ErrVersionConflict indicates different records with the same key and version.
var ErrVersionConflict = errors.New("conflicting record for version")

// ErrStaleRecord indicates that a newer version is already stored.
var ErrStaleRecord = errors.New("record version is stale")

// ErrVersionExhausted indicates that the logical counter cannot advance.
var ErrVersionExhausted = errors.New("version counter exhausted")

// MemoryStore holds versioned records and supports concurrent operations.
// Construct it with NewMemoryStore; do not copy it after use.
type MemoryStore struct {
	mu      sync.RWMutex
	nodeID  string
	counter uint64
	records map[string]Record
}

// NewMemoryStore creates a store whose local updates use nodeID.
func NewMemoryStore(nodeID string) (*MemoryStore, error) {
	if err := validateNodeID(nodeID); err != nil {
		return nil, fmt.Errorf("create memory store: %w", err)
	}
	return &MemoryStore{nodeID: nodeID}, nil
}

// Put inserts or replaces a value with a new local version.
func (store *MemoryStore) Put(key, value string) error {
	return store.writeLocal(Record{Key: key, Value: value})
}

// Get returns a live value; missing keys and deletion markers return false.
func (store *MemoryStore) Get(key string) (string, bool) {
	record, found := store.GetRecord(key)
	if !found || record.Deleted {
		return "", false
	}
	return record.Value, true
}

// GetRecord returns a copy of the record, including deletion markers.
func (store *MemoryStore) GetRecord(key string) (Record, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	record, found := store.records[key]
	return record, found
}

// Delete writes a new deletion marker, even when the key is absent.
func (store *MemoryStore) Delete(key string) error {
	return store.writeLocal(Record{Key: key, Deleted: true})
}

// Apply stores a newer record atomically; identical retries return false, nil.
// Stale records and conflicting contents return errors without changing storage.
func (store *MemoryStore) Apply(record Record) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	current, found := store.records[record.Key]
	if update, err := shouldApply(record, current, found); err != nil || !update {
		return false, err
	}
	store.counter = max(store.counter, record.Version.Counter)
	store.save(record)
	return true, nil
}

func (store *MemoryStore) writeLocal(record Record) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	version, err := store.nextVersion(Version{})
	if err != nil {
		return err
	}
	record.Version = version
	store.save(record)
	return nil
}

// NextVersion reserves a local version beyond observed without storing a value.
func (store *MemoryStore) NextVersion(observed Version) (Version, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.nextVersion(observed)
}

func (store *MemoryStore) nextVersion(observed Version) (Version, error) {
	if store.nodeID == "" {
		return Version{}, errors.New("local writes require a store created with NewMemoryStore")
	}
	store.counter = max(store.counter, observed.Counter)
	if store.counter == math.MaxUint64 {
		return Version{}, ErrVersionExhausted
	}
	store.counter++
	return Version{Counter: store.counter, NodeID: store.nodeID}, nil
}

// save requires the write lock; values and versions must change together.
func (store *MemoryStore) save(record Record) {
	if store.records == nil {
		store.records = make(map[string]Record)
	}
	store.records[record.Key] = record
}
