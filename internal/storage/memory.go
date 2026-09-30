package storage

import "sync"

// MemoryStore stores string values in memory and is safe for concurrent use.
// Its zero value is ready to use. A MemoryStore must not be copied after use.
// Individual operations are synchronized; sequences of operations are not atomic.
type MemoryStore struct {
	mu sync.RWMutex
	values map[string]string
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Put inserts or replaces the value for key. Empty keys and values are allowed.
func (store *MemoryStore) Put(key, value string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.values == nil {
		store.values = make(map[string]string)
	}
	store.values[key] = value
}

// Get returns the value for key and whether the key exists.
func (store *MemoryStore) Get(key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	value, found := store.values[key]
	return value, found
}

// Delete removes key. Deleting a missing key has no effect.
func (store *MemoryStore) Delete(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	delete(store.values, key)
}
