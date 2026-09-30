package storage_test

import (
	"fmt"
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
			store := storage.NewMemoryStore()
			store.Put(testCase.key, testCase.value)

			value, found := store.Get(testCase.key)
			if !found || value != testCase.value {
				t.Fatalf("Get(%q) = (%q, %t), want (%q, true)", testCase.key, value, found, testCase.value)
			}
		})
	}
}

func TestMemoryStoreMissingKey(t *testing.T) {
	store := storage.NewMemoryStore()

	value, found := store.Get("missing")
	if found || value != "" {
		t.Fatalf("Get(missing) = (%q, %t), want (\"\", false)", value, found)
	}
}

func TestMemoryStoreOverwritePreservesOtherKeys(t *testing.T) {
	store := storage.NewMemoryStore()
	store.Put("user:1", "Alice")
	store.Put("user:2", "Bob")
	store.Put("user:1", "Carol")

	for key, expected := range map[string]string{"user:1": "Carol", "user:2": "Bob"} {
		value, found := store.Get(key)
		if !found || value != expected {
			t.Errorf("Get(%q) = (%q, %t), want (%q, true)", key, value, found, expected)
		}
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	store := storage.NewMemoryStore()
	store.Put("user:1", "Alice")
	store.Put("user:2", "Bob")

	store.Delete("user:1")
	store.Delete("user:1")
	store.Delete("missing")

	if value, found := store.Get("user:1"); found || value != "" {
		t.Errorf("Get(deleted key) = (%q, %t), want (\"\", false)", value, found)
	}
	if value, found := store.Get("user:2"); !found || value != "Bob" {
		t.Errorf("Get(other key) = (%q, %t), want (\"Bob\", true)", value, found)
	}

	store.Put("user:1", "Carol")
	if value, found := store.Get("user:1"); !found || value != "Carol" {
		t.Errorf("Get(reinserted key) = (%q, %t), want (\"Carol\", true)", value, found)
	}
}

func TestMemoryStoreZeroValue(t *testing.T) {
	var store storage.MemoryStore
	store.Delete("missing")
	if value, found := store.Get("missing"); found || value != "" {
		t.Fatalf("Get(missing) = (%q, %t), want (\"\", false)", value, found)
	}

	store.Put("user:1", "Alice")
	if value, found := store.Get("user:1"); !found || value != "Alice" {
		t.Fatalf("Get(user:1) = (%q, %t), want (\"Alice\", true)", value, found)
	}
}

func TestMemoryStoreConcurrentOperations(t *testing.T) {
	store := storage.NewMemoryStore()
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
				store.Put(key, expected)
				if value, found := store.Get(key); !found || value != expected {
					t.Errorf("Get(%q) = (%q, %t), want (%q, true)", key, value, found, expected)
					return
				}
				store.Delete(key)
				if value, found := store.Get(key); found || value != "" {
					t.Errorf("Get(%q) after deletion = (%q, %t), want (\"\", false)", key, value, found)
					return
				}

				// Other workers may delete this key between Put and Get.
				store.Put("shared", "shared value")
				value, found := store.Get("shared")
				if (found && value != "shared value") || (!found && value != "") {
					t.Errorf("Get(shared) returned an invalid pair: (%q, %t)", value, found)
					return
				}
				store.Delete("shared")
			}
			store.Put(key, "finished")
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

func ExampleMemoryStore() {
	store := storage.NewMemoryStore()
	store.Put("user:1", "Alice")
	value, found := store.Get("user:1")
	fmt.Println(value, found)

	store.Delete("user:1")
	_, found = store.Get("user:1")
	fmt.Println(found)
	// Output:
	// Alice true
	// false
}
