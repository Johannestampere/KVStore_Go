# kvstore

A concurrent, in-memory key-value store with an HTTP API, written in Go using
only the standard library. This is the single-node milestone of a project that
will grow into a small Dynamo-style distributed system.

**Focus of this milestone:** concurrent storage, validated HTTP requests, and
unit and integration tests. Data is lost when the process stops; replication,
versioning, and persistence are future milestones.

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

The HTTP handler depends on a small storage interface. The node executable
connects it to `MemoryStore` and manages the server's lifecycle.

An immutable consistent-hash ring provides key ownership selection for the next
cluster milestone. It is not yet connected to HTTP routing; the running server
still stores every key locally.

## Quick start

Requires Go 1.27.0 or newer, matching `go.mod`.

Start the server:

```bash
go run ./cmd/node
```

The default address is `127.0.0.1:8001`. To use another address:

```bash
go run ./cmd/node -addr 127.0.0.1:8002
```

Run these commands in a second terminal:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/user:42 \
  -H 'Content-Type: application/json' \
  -d '{"value":"Alice"}'

curl -i http://127.0.0.1:8001/kv/user:42

curl -i -X DELETE http://127.0.0.1:8001/kv/user:42

curl -i http://127.0.0.1:8001/kv/user:42
```

Expect `204`, `200` with `{"key":"user:42","value":"Alice"}`, `204`, then `404`.

For a standalone executable:

```bash
go build -o /tmp/kvstore-node ./cmd/node
/tmp/kvstore-node -addr 127.0.0.1:8001
```

Ctrl+C or SIGTERM stops new connections and allows active requests up to ten
seconds to finish, then closes remaining connections. The server uses a
five-second header timeout, ten-second read/write timeouts, and a sixty-second
idle timeout. Startup and shutdown failures exit with a nonzero status.

## HTTP API

| Request | Success | Error behavior |
| --- | --- | --- |
| `PUT /kv/{key}` | `204 No Content` | `400` for invalid JSON; `413` for an oversized body; `415` for an unsupported content type |
| `GET /kv/{key}` | `200` with JSON key and value | `404` when the key is missing |
| `DELETE /kv/{key}` | `204 No Content`, including missing keys | No error for an already deleted key |

PUT requires `Content-Type: application/json` and one JSON object containing a
string `value`. Empty values are accepted; missing or null values, unknown
fields, and extra JSON values are rejected before storage is changed. The
request-body limit is 1 MiB including JSON overhead.

Keys must occupy one non-empty URL path segment. URL-escape reserved characters;
for example, the key `user/42` uses `/kv/user%2F42`. Handler errors use
`{"error":"message"}`. Unknown routes and unsupported methods use the standard
router's responses. GET routes also support HEAD through Go's HTTP server.

## Tests

Unit tests exercise storage, HTTP behavior, and consistent hashing independently.
Integration tests connect the real handler and store through a local HTTP server,
covering key lifecycles, rejected writes, and concurrent clients.

```bash
go vet ./...
go test -race ./...
go test -race ./tests/unit/...
go test -race ./tests/integration/...
```

Add `-v` to list individual test cases. The race detector reports unsynchronized
access exercised during a test run; a passing run is not proof that every possible
execution is race-free. It requires a C compiler, such as the one supplied by
Apple's Command Line Tools on macOS.

## Layout

```text
cmd/node/main.go                 Server startup and shutdown
internal/api/handler.go          HTTP routing and validation
internal/consistenthash/ring.go   Deterministic key ownership
internal/storage/memory.go       Concurrent in-memory storage
tests/unit/api/                  Handler tests with a storage spy
tests/unit/consistenthash/       Hash-ring behavior and concurrency tests
tests/unit/storage/              Storage tests
tests/integration/api/           HTTP tests with real storage
```

## Storage contract

| Operation | Behavior |
| --- | --- |
| `Put(key, value)` | Insert or overwrite |
| `Get(key)` | `(value, true)` if present, `("", false)` if missing |
| `Delete(key)` | Remove the key; missing key is a no-op |

Empty keys and values are allowed. The `found` boolean is the only reliable way to distinguish a stored empty string from a missing key. Operations are individually synchronized; there are no multi-key transactions.

## Consistent hashing

`consistenthash.NewRing(nodeIDs, virtualNodes)` constructs a ring from unique,
non-empty node IDs. Each physical node receives the same positive number of
virtual positions. More positions provide additional opportunities to spread
ownership across nodes; they do not guarantee equal load.

`ring.GetNodes(key, count)` returns distinct physical node IDs in clockwise order.
The first ID is the primary owner; subsequent IDs are candidate replicas.
The count must be between one and the number of physical nodes. Empty keys are
valid at this layer. Duplicate IDs, invalid virtual-node counts, and impossible
replica counts return errors.

Placement uses the first eight bytes of SHA-256, interpreted as a big-endian
unsigned integer. Keys are hashed directly. Virtual positions hash the node ID,
a NUL byte, and the zero-based decimal virtual-node index. Positions are sorted
numerically, with node IDs breaking hash ties. All nodes must use the same IDs,
virtual-node count, and hashing rules to agree on ownership.

Lookups use binary search, wrap around the ring, and skip previously selected
owners. The ring is immutable after construction and returns fresh result slices,
so concurrent lookups need no locks. Construct a new ring to represent a different
membership; this does not migrate stored values or provide live membership changes.

The tests cover fixed routing examples, exact hash boundaries, wraparound,
input-order independence, unique owners, caller mutation, and concurrent reads.
Membership tests verify that adding a node only moves primary ownership to that
node, and removing a node preserves primary ownership on remaining nodes.
