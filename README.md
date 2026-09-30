# kvstore

A concurrent, in-memory key-value store written in pure Go. This is the first milestone of a project that will grow into a small Dynamo-style distributed system (HTTP API, replication, versioning, and persistence).

**Focus of this milestone:** correctness under concurrency, clear API design, and thorough testing. No external dependencies.

```go
store := storage.NewMemoryStore()
store.Put("user:42", "alice")
value, ok := store.Get("user:42") // "alice", true
store.Delete("user:42")
```

## Why this exists

Most toy KV stores stop at a map + mutex. This one is written to be a solid foundation:

- Explicit storage contract that distinguishes missing keys from empty values
- Proper use of `sync.RWMutex` (readers don’t block each other)
- Race-detector-clean tests that exercise both contended and uncontended paths
- Zero-value safety and careful handling of edge cases (empty keys/values, Unicode)

It is deliberately a **library**, not a server. Future milestones will layer HTTP, replication, and durability on top of this core.

## Quick start

Requires Go 1.22+ (or newer).

```bash
go test -race ./...
go test -race -v ./...   # see individual test names
```

The race detector is required; the tests are written to surface data races if the locking is ever broken.

## Storage contract

| Operation | Behavior |
| --- | --- |
| `Put(key, value)` | Insert or overwrite |
| `Get(key)` | `(value, true)` if present, `("", false)` if missing |
| `Delete(key)` | Remove the key; missing key is a no-op |

Empty keys and values are allowed. The `found` boolean is the only reliable way to distinguish a stored empty string from a missing key. Operations are individually synchronized; there are no multi-key transactions.
