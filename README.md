# kvstore

A partitioned, in-memory key-value store with an HTTP API, written in Go using
only the standard library. Any configured node coordinates requests across
three replicas selected by a shared consistent-hash ring. Reads and writes
currently require every selected replica to respond successfully.

```go
store, err := storage.NewMemoryStore("node-a")
if err != nil {
    return err
}
if err := store.Put("user:42", "alice"); err != nil {
    return err
}
value, ok := store.Get("user:42") // "alice", true
if err := store.Delete("user:42"); err != nil {
    return err
}
```

## Why this exists

Most toy KV stores stop at a map + mutex. This one is written to be a solid foundation:

- Explicit storage contract that distinguishes missing keys from empty values
- Proper use of `sync.RWMutex` (readers don’t block each other)
- Race-detector-clean tests that exercise both contended and uncontended paths
- Versioned records and deletion markers that reject stale updates
- Careful handling of edge cases (empty keys/values, Unicode)

The HTTP handler depends on a small service interface with cancellation and
error support. The node executable connects it to local storage or cluster
replication and manages the server's lifecycle.

An immutable consistent-hash ring selects key owners, and a static membership
directory maps node IDs to HTTP addresses. Startup can load both from a shared
JSON file. Each key lives on three distinct nodes by default. The node receiving
a public request coordinates local access and concurrent peer HTTP calls.

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

Keys must be valid UTF-8 (`400` otherwise) and occupy one non-empty URL path
segment, with at most 4 KiB of decoded
key bytes (`414` if exceeded). URL-escape reserved characters;
for example, the key `user/42` uses `/kv/user%2F42`. Handler errors use
`{"error":"message"}`. Unknown routes and unsupported methods use the standard
router's responses. GET routes also support HEAD through Go's HTTP server.
Keys consisting of `.` or `..` must use `%2E` or `%2E%2E` to avoid path cleaning.

In cluster mode, operations can return `503` for an unavailable replica,
`504` for a deadline, `502` for an invalid peer response, or `409` for a stale
write or conflicting records with the same version. GET returns `404` only
after all replicas answer and either none has a record or the highest version
is a deletion marker. A `404` peer response counts as a successful missing-key
lookup, not as an unavailable replica.

## Cluster startup configuration

`configs/cluster.local.json` lists five local nodes on ports 8001–8005 with 64
virtual positions per node and `replication_factor: 3`. Start all five in
separate terminals:

```bash
go run ./cmd/node -config configs/cluster.local.json -id node-a -addr 127.0.0.1:8001
go run ./cmd/node -config configs/cluster.local.json -id node-b -addr 127.0.0.1:8002
go run ./cmd/node -config configs/cluster.local.json -id node-c -addr 127.0.0.1:8003
go run ./cmd/node -config configs/cluster.local.json -id node-d -addr 127.0.0.1:8004
go run ./cmd/node -config configs/cluster.local.json -id node-e -addr 127.0.0.1:8005
```

Write through one node and read or delete through another:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/user:42 \
  -H 'Content-Type: application/json' -d '{"value":"Alice"}'
curl -i http://127.0.0.1:8003/kv/user:42
curl -i -X DELETE http://127.0.0.1:8005/kv/user:42
```

Expect `204`, `200` with Alice's value, then `204`. All three requests use the
same replica set. After PUT, inspect local records on ports 8001–8005 with
`GET /internal/records/user:42`: exactly three nodes hold the same record;
the other two return `404`. After DELETE, those three nodes hold matching
tombstones, while public GET returns `404`.

Starting only part of the configured cluster leaves keys assigned to an offline
replica unavailable under this milestone's requirement for all replicas.

| Flag | Purpose | Default |
| --- | --- | --- |
| `-addr` | Local listening address | `127.0.0.1:8001` |
| `-config` | Shared cluster JSON file | No cluster configuration |
| `-id` | Local member ID from that file | None |
| `-peer-timeout` | Positive timeout for each outgoing peer request | `2s` |
| `-request-timeout` | Deadline for a complete coordinated operation | `5s` |

`-config` and `-id` must be supplied together. The local ID must exist in the
member list. `-addr` remains independent of the advertised member address and
does not change automatically when selecting a different ID. This separation
allows a process to bind a local interface while advertising a peer-reachable
address. The server currently listens using plain HTTP.

The configuration requires a `members` array and a positive `virtual_nodes`
integer. `replication_factor` defaults to 3 and must be between one and the
member count; a smaller test cluster must set it explicitly. All nodes must
agree on this value. `-request-timeout` must be positive and below the server's
10-second write timeout. Each member has `id` and `address` fields. Unknown fields, malformed JSON,
extra JSON values, invalid membership, and invalid ring settings fail startup
before binding the listening address. The file is read once; changes require
restarting the process. Every node must use the same topology settings.

Omit both cluster flags to retain the original single-node startup behavior.

## Replication

```text
Client → public /kv/{key} → Coordinator
                              ↓
                    Select three distinct replicas
                       /        |        \
                      v         v         v
                   Node A     Node C     Node E
                 local store or /internal/records/{key}
```

The receiving node acts as the coordinator, even when it is outside the key's
replica set. `Coordinator.Put` and `Delete` perform two phases:

1. Read every selected replica concurrently and observe the highest version.
2. Reserve one higher version locally, then send the identical record to every
   selected replica concurrently. DELETE sends a tombstone through the same path.

A write returns success only after all replicas accept the record or recognize
an identical retry. A stale record returns `409`; ignoring a stale record is
not counted as a successful write. Concurrent coordinators can observe the
same counter; their node IDs break the tie. One writer may receive a conflict
if another update overtakes it.

`Coordinator.Get` reads every replica concurrently and returns the highest
version, treating a winning tombstone as missing. Identical versions containing
different values or deletion flags produce a conflict. Reads do not repair
older or missing replicas yet.

Replica calls run in **goroutines**, Go's lightweight concurrent tasks. Results
arrive through a buffered **channel**, allowing workers to finish even if the
request is canceled before their results are collected. `context.Context`
propagates cancellation and deadlines. The operation deadline covers both
observation and writing; the peer timeout also bounds each HTTP exchange.

`HTTPNodeClient.GetRecord` and `Apply` exchange complete versioned records,
validate responses, reuse connections, and refuse redirects. Cluster mode
exposes `GET` and `PUT /internal/records/{key}`. These endpoints access local
storage without forwarding or assigning a new version. Deletion is represented
by a record with `deleted: true`, so there is no internal DELETE endpoint.
The former `/internal/kv/{key}` protocol has been replaced.

Internal JSON bodies and peer responses are limited to 8 MiB to accommodate
escaping of public values. Incoming records require all four fields: `key`,
`value`, `version`, and `deleted`. The value is limited to 1 MiB of decoded
bytes, and the record key must match the URL. Missing local records return
`404`; stored tombstones return `200` with the complete record.

There are no automatic retries or fallback replicas. Membership stays fixed
when a node fails. A failed observation phase sends no writes. A failure or
timeout during the write phase can leave partial writes; there is no rollback,
and subsequent reads may expose them. The system does not claim linearizability
or automatic convergence. Restart recovery, persistence, and replica repair
remain unfinished. Quorum reads and writes are the next milestone.

Standalone mode still uses `LocalService` and exposes only the public API.

## Tests

Unit tests exercise storage, HTTP behavior, consistent hashing, membership,
configuration loading, replica selection, and the peer protocol.
Storage tests cover version ordering, duplicate and conflicting updates,
deletion markers, counter exhaustion, and concurrent version assignment.
Integration tests run real HTTP servers and storage, covering cross-node key
lifecycles, identical three-copy placement, concurrent clients, escaped keys,
large values, unavailable replicas, timeouts, partial writes, and internal
endpoint isolation. Coordinator tests also cover parallel fan-out, cancellation,
version observation, and simultaneous writers with tied counters.

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
internal/api/handler.go          Public HTTP validation and error responses
internal/api/replica.go          Node-local versioned record endpoints
internal/cluster/membership.go   Static node IDs and HTTP addresses
internal/config/config.go        Shared JSON configuration and local identity
internal/consistenthash/ring.go   Deterministic key ownership
internal/routing/local.go        Local storage adapter with cancellation support
internal/replication/coordinator.go  Parallel replica reads and writes
internal/replication/record.go       Strict peer record decoding
internal/transport/http.go       Bounded peer HTTP client
internal/storage/memory.go       Concurrent versioned storage and logical counter
internal/storage/record.go       Records, deletion markers, and version ordering
configs/cluster.local.json       Five-node local topology
tests/unit/api/                  Handler tests with a service spy
tests/unit/cluster/              Membership validation and lookup tests
tests/unit/config/               Configuration loading tests
tests/unit/consistenthash/       Hash-ring behavior and concurrency tests
tests/unit/storage/              Storage tests
tests/unit/routing/              Local service cancellation and errors
tests/unit/replication/          Replica coordination and failure tests
tests/unit/transport/            Peer protocol and cancellation tests
tests/integration/api/           HTTP tests with real storage
tests/integration/replication/   Five-node replication tests
```

## Storage contract

| Operation | Behavior |
| --- | --- |
| `Put(key, value)` | Insert or overwrite with a new local version; returns an error on failure |
| `Get(key)` | `(value, true)` if present, `("", false)` if missing |
| `Delete(key)` | Write a new deletion marker, including for absent keys; returns an error on failure |
| `GetRecord(key)` | Return the complete record, including deletion markers, and a found flag |
| `Apply(record)` | Atomically accept a newer record; reject stale/conflicting records; identical retries succeed without changes |
| `NextVersion(observed)` | Reserve a version higher than the observed and local counters without storing a record |

Empty keys and values are allowed. The `found` boolean is the only reliable way to distinguish a stored empty string from a missing key. Operations are individually synchronized; there are no multi-key transactions.

Construct storage with `NewMemoryStore(nodeID)`, which validates the writer's
identity. A zero-value store rejects local writes because it has no node ID.
Cluster startup uses the configured ID; standalone startup uses `standalone`.
Public successful responses retain the original format.

## Versioned records

Each `Record` contains a key, string value, `Version`, and `Deleted` flag.
Versions compare the unsigned logical counter first, then the node ID in lexical
order. For example, `(8, node-a)` follows `(7, node-z)`, and `(8, node-b)` wins a
tie with `(8, node-a)`. These rules give all replicas the same comparison result;
they do not measure wall-clock time or establish strong consistency.

The store maintains one counter across keys. A local PUT or DELETE increments
it under the same lock that updates the record. Applying a remote record raises
the counter to at least the received value, so the next local update follows
every version the store has accepted. Counter overflow returns an error without
changing stored records.

`Apply` rejects older records with `ErrStaleRecord`; identical retries succeed
without changes. A different value or
deletion flag with the same key and exact version returns `ErrVersionConflict`;
one version must identify one update. Invalid records are rejected before
storage changes. Records contain strings and value fields, so returned copies
cannot mutate storage.

DELETE retains a **tombstone**, a record with `Deleted: true` and an empty value.
`Get` treats it as missing, while `GetRecord` exposes it to replication.
An older value cannot overwrite that marker; a newer PUT can recreate the key.
Repeated deletes create newer markers even when the key is already absent.
Tombstones are retained indefinitely for now and consume memory.

The coordinator uses `NextVersion` to reserve a version without storing an
extra copy on a node outside the replica set. Reservations and local writes
share the same locked counter. Failed operations can leave gaps in that counter;
gaps are harmless. Every replica receives the reserved version unchanged.

Records and counters are currently in memory. Restarting a node loses both and
can reuse versions under the same node ID. Before supporting replication across
restarts, recovery must restore the counter and records or introduce a durable
writer epoch. There is no replica repair or convergence mechanism yet.

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
Ring membership-change tests verify that adding a node only moves primary
ownership to that node, and removing a node preserves primary ownership on
remaining nodes.

## Static cluster membership

`cluster.Member` contains an `ID` and an `Address`, such as `node-a` and
`http://127.0.0.1:8001`. `cluster.NewMembership(members)` validates and copies
the supplied members into an immutable directory.

- Membership must contain at least one node. IDs must be unique, non-empty,
  and free of surrounding whitespace.
- Addresses must be absolute HTTP or HTTPS base URLs with a host. An optional
  port must be between 1 and 65535. IPv6 hosts must use brackets.
- An optional root slash is accepted. Credentials, application paths, queries,
  and fragments are rejected. Accepted address strings are preserved as supplied.

`Lookup(nodeID)` returns `(Member, bool)`, where the boolean indicates whether
the ID exists. `NodeIDs()` returns a sorted copy suitable for constructing a
hash ring. Returned members and slices can be changed without modifying the
directory; concurrent reads need no locks.

Validation checks configuration syntax without DNS lookups or network requests.
It does not verify reachability or whether different addresses refer to the same
server. Offline nodes remain members. Startup loads the directory, identifies
the local node, and connects the shared topology to replication.
