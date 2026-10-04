package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

// ErrVersionConflict indicates different records with the same key and version.
var ErrVersionConflict = errors.New("conflicting record for version")

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

// Apply stores a newer record atomically. Stale or identical records return false.
// Reusing a version for different contents returns ErrVersionConflict.
func (store *MemoryStore) Apply(record Record) (bool, error) {
	if err := record.Validate(); err != nil {
		return false, fmt.Errorf("apply record %q: %w", record.Key, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	if current, found := store.records[record.Key]; found {
		switch record.Version.Compare(current.Version) {
		case -1:
			return false, nil
		case 0:
			if record != current {
				return false, fmt.Errorf("apply record %q: %w", record.Key, ErrVersionConflict)
			}
			return false, nil
		}
	}
	store.counter = max(store.counter, record.Version.Counter)
	store.save(record)
	return true, nil
}

func (store *MemoryStore) writeLocal(record Record) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.nodeID == "" {
		return errors.New("local writes require a store created with NewMemoryStore")
	}
	if store.counter == math.MaxUint64 {
		return ErrVersionExhausted
	}
	store.counter++
	record.Version = Version{Counter: store.counter, NodeID: store.nodeID}
	store.save(record)
	return nil
}

// save requires the write lock; values and versions must change together.
func (store *MemoryStore) save(record Record) {
	if store.records == nil {
		store.records = make(map[string]Record)
	}
	store.records[record.Key] = record
}
