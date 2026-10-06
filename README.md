# kvstore

A partitioned key-value store with optional append-only persistence and an HTTP
API, written in Go using
only the standard library. Any configured node coordinates requests across
three replicas selected by a shared consistent-hash ring. Reads and writes
use configurable quorums: by default, two successful responses out of three.

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

Requires Go 1.27.0 or newer, matching `go.mod`. Persistent storage currently
targets macOS and Linux local filesystems.

Start the server:

```bash
go run ./cmd/node -data-dir data/standalone
```

The default address is `127.0.0.1:8001`. To use another address:

```bash
go run ./cmd/node -addr 127.0.0.1:8002 -data-dir data/standalone-8002
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
seconds to finish, then closes remaining connections. Shutdown then drains
outstanding replica calls for up to ten seconds, canceling them if that budget
expires. The server uses a
five-second header timeout, ten-second read/write timeouts, and a sixty-second
idle timeout. Startup and shutdown failures exit with a nonzero status.

## HTTP API

| Request | Success | Error behavior |
| --- | --- | --- |
| `PUT /kv/{key}` | `204 No Content` | `400` for invalid JSON; `413` for an oversized body; `415` for an unsupported content type |
| `GET /kv/{key}` | `200` with JSON key and value | `404` when the key is missing |
| `DELETE /kv/{key}` | `204 No Content`, including missing keys | No error for an already deleted key |
| `GET /health` | `200` with `{"status":"ok"}` | Reports process liveness only |

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

In cluster mode, operations can return `503` when too few replicas are available,
`504` for a deadline, `502` for an invalid peer response, or `409` for a stale
write or conflicting records with the same version. GET returns `404` only
after a read quorum answers and either none of those responses has a record or
the highest returned version is a deletion marker. A `404` peer response counts as a successful missing-key
lookup, not as an unavailable replica.

## Health checks

Both standalone and cluster nodes expose `GET /health`. It returns JSON with
`Cache-Control: no-store`; `HEAD /health` returns the same status without a body.
Other methods return `405` with `Allow: GET, HEAD`.

```bash
curl -i http://127.0.0.1:8001/health
```

Health means the process can handle this HTTP request. It does not check disk
writability, contact peers, or guarantee quorum. A node can return `200` here
while key-value requests fail with `503`. The listener opens after storage
recovery, so probes cannot succeed while the log is still replaying.

Health status does not change the hash ring or replace unavailable replicas.
There is no peer-health polling or automatic repair in this milestone.

## Docker cluster

With Docker Engine and Docker Compose running, start all five nodes:

```bash
docker compose up --build
```

For background operation with a wait for healthy containers:

```bash
docker compose up --build -d --wait
docker compose ps
```

Stop any manually launched nodes using ports 8001–8005 first. Containers listen
on port 8000 internally and expose host ports 8001–8005 on `127.0.0.1`.
`configs/cluster.docker.json` uses service names such as `http://node-a:8000`
for peer traffic. It keeps the same node IDs, ring settings, and `N=3, R=2, W=2`
as the local configuration. Host loopback addresses cannot identify other
containers from inside a container.

The Dockerfile builds the executable in a Go image and copies it into a smaller
Alpine runtime image. The process runs as the `kvstore` user. Each service has
its own named volume mounted at `/var/lib/kvstore`, and its log remains bound
to that node's identity. `.dockerignore` limits build input to application
source, the Go module, and Docker cluster configuration; local instructions,
Git history, tests, and data directories are excluded.

Compose probes `/health` every five seconds and allows thirty seconds for
graceful shutdown before forcing termination. A health failure changes the
container's reported status; this configuration does not automatically restart
unhealthy containers or change replica placement. See the
[Compose service reference](https://docs.docker.com/reference/compose-file/services/)
for the health-check and shutdown settings.

### Failure and restart demo

With the supplied topology, `node-c` is a replica for both keys below. Write
one key to preserve across restart and another to exercise quorum operations:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/demo:persist \
  -H 'Content-Type: application/json' -d '{"value":"survives restart"}'
curl -i -X PUT http://127.0.0.1:8002/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"before failure"}'
curl -i http://127.0.0.1:8003/internal/records/demo:persist
```

Before stopping the node, confirm the last request returns `200` with the saved
record. If it returns `404`, retry after the outstanding replica write finishes.
Then stop `node-c` and operate through a remaining node:

```bash
docker compose stop node-c
curl -i http://127.0.0.1:8002/kv/demo:quorum
curl -i -X PUT http://127.0.0.1:8002/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"during failure"}'
curl -i http://127.0.0.1:8002/kv/demo:quorum
curl -i -X DELETE http://127.0.0.1:8002/kv/demo:quorum
curl -i http://127.0.0.1:8002/kv/demo:quorum
```

Expect `200`, `204`, `200` with the updated value, `204`, and `404`.
Restart the stopped replica with its existing volume:

```bash
docker compose up -d --wait node-c
curl -i http://127.0.0.1:8003/internal/records/demo:persist
curl -i http://127.0.0.1:8002/kv/demo:quorum
```

The local record still contains `survives restart`, and the quorum read of the
deleted key returns `404`. Recovery restores the node's own history; updates
missed while it was stopped are not automatically repaired. Newer tombstones
on the other replicas win over an older value during a quorum read.

`docker compose down` removes containers and the network while retaining named
volumes. Running `docker compose up -d --wait` recreates the containers with
their saved records and counters. `docker compose down --volumes` also deletes
the stored data; use it only when intentionally resetting this cluster.

## Cluster startup configuration

`configs/cluster.local.json` lists five local nodes on ports 8001–8005 with 64
virtual positions per node, `replication_factor: 3`, `read_quorum: 2`, and
`write_quorum: 2`. Start all five in
separate terminals:

```bash
go run ./cmd/node -config configs/cluster.local.json -id node-a -addr 127.0.0.1:8001 -data-dir data/node-a
go run ./cmd/node -config configs/cluster.local.json -id node-b -addr 127.0.0.1:8002 -data-dir data/node-b
go run ./cmd/node -config configs/cluster.local.json -id node-c -addr 127.0.0.1:8003 -data-dir data/node-c
go run ./cmd/node -config configs/cluster.local.json -id node-d -addr 127.0.0.1:8004 -data-dir data/node-d
go run ./cmd/node -config configs/cluster.local.json -id node-e -addr 127.0.0.1:8005 -data-dir data/node-e
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
`GET /internal/records/user:42`: the three selected nodes should hold the same
record once outstanding writes finish; the other two return `404`. A successful
PUT guarantees at least two acknowledgements. After DELETE, acknowledged
replicas hold a tombstone, while public GET returns `404`.

With the default quorums, one unavailable replica does not prevent reads or
writes. Two unavailable replicas for the same key prevent quorum. Ownership
does not change when a node is offline.

| Flag | Purpose | Default |
| --- | --- | --- |
| `-addr` | Local listening address | `127.0.0.1:8001` |
| `-config` | Shared cluster JSON file | No cluster configuration |
| `-id` | Local member ID from that file | None |
| `-data-dir` | Exclusive node-local log directory | Empty: memory only |
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
agree on placement and quorum settings. `read_quorum` and `write_quorum` each
default to a majority (`replication_factor / 2 + 1`). Both must be between 1
and the replication factor, and their sum must exceed the replication factor.
Explicit zero, null, and non-overlapping settings are rejected. Set both to the
replication factor to require every replica.

`-request-timeout` must be positive and below the server's
10-second write timeout. Each member has `id` and `address` fields. Unknown fields, malformed JSON,
extra JSON values, invalid membership, and invalid ring settings fail startup
before binding the listening address. The file is read once; changes require
restarting the process. Every node must use the same topology settings.

Omit both cluster flags for standalone mode. Supply `-data-dir` in either mode
to preserve local data across restarts; omitting it keeps the in-memory behavior.

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

1. Contact all selected replicas concurrently, wait for `read_quorum` valid
   responses, and observe the highest version among them.
2. Reserve one higher version locally, then attempt the identical record on
   every selected replica concurrently. Return after `write_quorum` accepts it.
   DELETE sends a tombstone through the same path.

A replica acknowledgement means the record was accepted or recognized as an
identical retry. With `-data-dir`, new records are synchronized to disk before
acknowledgement. Stale or conflicting records do not count toward quorum; an
operation that cannot obtain enough acknowledgements can return `409` when
version conflicts are the cause. Concurrent coordinators can observe the
same counter; their node IDs break the tie. One writer may receive a conflict
if another update overtakes it.

`Coordinator.Get` contacts every replica and returns the highest version among
the first `read_quorum` valid responses, treating a winning tombstone as missing.
A valid missing-key response counts toward quorum; malformed responses and
network errors do not. Conflicting contents with the same version among the
selected responses produce a conflict. Remaining reads are canceled after
quorum succeeds or becomes impossible. Reads do not repair older or missing
replicas yet.

Replica calls run in **goroutines**, Go's lightweight concurrent tasks. Results
arrive through a buffered **channel**, allowing workers to finish even if the
request is canceled before their results are collected. `context.Context`
propagates cancellation and deadlines. The operation deadline covers both
observation and writing; the peer timeout also bounds each HTTP exchange.
Quorum collection stops as soon as enough successes arrive or the remaining
responses cannot make success possible.

After write quorum succeeds, outstanding writes continue even after the client
request ends. They keep the original operation deadline; there is no timeout
extension. Before quorum succeeds, caller cancellation or quorum failure cancels
outstanding writes. Background failures are logged. `Coordinator.Shutdown`
stops admission and drains active requests and replica calls; an expired shutdown
deadline cancels remaining work.

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
or automatic convergence. Persistence recovers each node's own log; replica
repair remains unfinished. Quorum overlap (`R + W > N`) applies within the unchanged
replica set; it is not a proof of strong consistency. A newer partial write can
be visible to one read quorum and absent from another. Version observation uses
only its responding quorum, so an unobserved newer version can also reject a
subsequent write on a replica. Failed or canceled writes may still have effects.

Standalone mode uses `LocalService` and exposes the public key-value API and
`/health`, without internal replication endpoints.

## One-node failure demo

Start all five nodes using the sample configuration, then write a demo key:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"before failure"}'

for port in 8001 8002 8003 8004 8005; do
  curl -i "http://127.0.0.1:${port}/internal/records/demo:quorum"
done
```

Choose one node returning a record and stop it with Ctrl+C in its terminal.
Keep the configuration unchanged. Run these commands against any remaining
node, substituting its listening port if needed:

```bash
curl -i http://127.0.0.1:8002/kv/demo:quorum
curl -i -X PUT http://127.0.0.1:8002/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"during failure"}'
curl -i http://127.0.0.1:8002/kv/demo:quorum
curl -i -X DELETE http://127.0.0.1:8002/kv/demo:quorum
curl -i http://127.0.0.1:8002/kv/demo:quorum
```

Expect `200`, `204`, `200` with the updated value, `204`, and `404`.
Stopping a second node in that key's replica set makes requests fail with `503`
when connection failures are detected, or `504` if requests time out. Restarting
a node with the same `-data-dir` restores its synchronized local history,
including tombstones. It does not repair updates missed while offline.

## Persistence and recovery

`-data-dir` enables `PersistentStore`; each node must use its own directory and
retain it across restarts. The log records the node identity and rejects a
mismatched `-id`. An advisory file lock prevents two processes from opening the
same log for writing. The lock is released when the process exits, including a
crash. The sample directories under `data/` are excluded from Git.

A single writer goroutine serializes mutations through a channel. It appends a
log entry, calls `File.Sync`, then publishes the change to the in-memory map.
Reads use a short memory lock and continue while disk I/O is pending. PUT,
DELETE, and incoming replica updates follow this ordering. Identical retries
need no new entry; stale or conflicting updates are rejected.

`NextVersion` also synchronizes reservations before returning. This preserves
the coordinator's counter even when it is outside the replica set and stores
no copy of the value. Replay restores both the records and the highest reserved
counter. A failed operation may leave counter gaps, which are harmless.

`store.log` starts with format and node-identity metadata. Each frame has a
12-byte header containing a payload length, payload CRC32, and header CRC32.
Payloads use Go's binary `gob` encoding, preserving string bytes exactly, and
are limited to 16 MiB. A damaged length fails the header checksum before it can
be mistaken for an incomplete final entry.

Startup replays the log before accepting HTTP requests. A partial final header
or payload is truncated to the last complete frame, and the truncation is
synchronized before new writes. Complete frames with invalid checksums, metadata,
or record/counter ordering fail startup instead of silently discarding data.

After an append or synchronization error, mutations fail until the store is
reopened. The failed entry is not published in memory, but may survive recovery;
a failed response never proves that a write had no effect. Reads still expose
the last successfully published state. The `Journal` interface isolates this
storage boundary so tests can simulate failures and delayed synchronization.

Shutdown drains HTTP requests and replication before closing storage. If the
replication drain times out, it cancels outstanding work and closes storage to
new mutations. An already admitted disk operation completes before close.
Filesystem writes and synchronization are not context-cancellable, so stalled
local disk I/O can outlast HTTP and shutdown deadlines.

To verify standalone recovery, start the quick-start command, write a key,
stop the process, then restart with the same directory and GET the key again.
The integration tests also verify acknowledged values after forcibly killing
the executable, and ensure deleted values remain deleted.

There is no log compaction, snapshotting, or replica repair yet. All current
records and tombstones remain in memory; historical entries remain on disk and
increase startup replay work. `File.Sync` requests OS-level synchronization;
power-loss behavior still depends on the filesystem and device. This project
does not claim production-grade power-loss durability. Removing the data
directory loses both records and the version history for that node identity.

## Tests

Unit tests exercise storage, HTTP behavior, consistent hashing, membership,
configuration loading, replica selection, and the peer protocol.
Storage tests cover version ordering, duplicate and conflicting updates,
deletion markers, counter exhaustion, and concurrent version assignment.
Integration tests run real HTTP servers and storage, covering cross-node key
lifecycles, identical three-copy placement, concurrent clients, escaped keys,
large values, unavailable replicas, timeouts, partial writes, and internal
endpoint isolation. Coordinator tests also cover parallel fan-out, cancellation,
version observation, and simultaneous writers with tied counters. Quorum tests
exercise one-node availability, impossible quorums, invalid responses, missing
keys, post-response replication, caller cancellation, original deadlines, and
shutdown draining. Tests also retain the stricter three-of-three policy.
Persistence tests cover replay, tombstones, unstored version reservations,
concurrent mutations, uncertain disk errors, incomplete tails, corruption,
exclusive access, identity checks, and graceful/crash restarts of the compiled
node in standalone and single-member cluster modes.
Health tests cover GET, HEAD, unsupported methods, and a running cluster node
whose key-value requests cannot obtain quorum. Executable lifecycle tests live
under `tests/integration/node/`; log recovery tests remain under storage.

```bash
go vet ./...
go test -race ./...
go test -race ./tests/unit/...
go test -race ./tests/integration/...
docker compose config --quiet
```

Add `-v` to list individual test cases. The race detector reports unsynchronized
access exercised during a test run; a passing run is not proof that every possible
execution is race-free. It requires a C compiler, such as the one supplied by
Apple's Command Line Tools on macOS.

The Go tests do not require Docker. The Docker demo above separately exercises
container networking, health probes, failure handling, and persistent volumes.

## Layout

```text
cmd/node/main.go                 Server startup and shutdown
internal/api/handler.go          Public HTTP validation and error responses
internal/api/health.go           Process liveness endpoint
internal/api/replica.go          Node-local versioned record endpoints
internal/cluster/membership.go   Static node IDs and HTTP addresses
internal/config/config.go        Shared JSON configuration and local identity
internal/consistenthash/ring.go   Deterministic key ownership
internal/routing/local.go        Local storage adapter with cancellation support
internal/replication/coordinator.go  Parallel replica reads and writes
internal/replication/record.go       Strict peer record decoding
internal/replication/quorum.go       Quorum validation and result collection
internal/replication/lifecycle.go    Cancellation and graceful draining
internal/transport/http.go       Bounded peer HTTP client
internal/storage/memory.go       Concurrent versioned storage and logical counter
internal/storage/record.go       Records, deletion markers, and version ordering
internal/storage/persistent.go   Serialized durable mutations and memory reads
internal/storage/journal.go      Durable entry and journal contract
internal/storage/log.go          Framing, checksums, exclusive locking, and replay
configs/cluster.local.json       Five-node local topology
configs/cluster.docker.json      Five-node topology using Docker service names
Dockerfile                      Go build and non-root runtime image
docker-compose.yml              Five services, health checks, and local volumes
.dockerignore                   Restricted Docker build context
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
tests/integration/storage/       Log recovery tests
tests/integration/node/          Executable health and restart tests
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

Construct volatile storage with `NewMemoryStore(nodeID)` or durable storage
with `OpenPersistentStore(directory, nodeID)`. Both validate the writer's identity. A zero-value store rejects local writes because it has no node ID.
Cluster startup uses the configured ID; standalone startup uses `standalone`.
Public successful responses retain the original format.

## Versioned records

Each `Record` contains a key, string value, `Version`, and `Deleted` flag.
Versions compare the unsigned logical counter first, then the node ID in lexical
order. For example, `(8, node-a)` follows `(7, node-z)`, and `(8, node-b)` wins a
tie with `(8, node-a)`. These rules give all replicas the same comparison result;
they do not measure wall-clock time or establish strong consistency.

The store maintains one counter across keys. A local PUT or DELETE advances
it together with the record: under a lock in `MemoryStore`, or through the
serialized journal writer in `PersistentStore`. Applying a remote record raises
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
share the same serialized counter. Failed operations can leave gaps in that counter;
gaps are harmless. Every replica receives the reserved version unchanged.

With `-data-dir`, restart recovery restores records, tombstones, and reserved
counters before serving requests. Memory-only mode loses those on restart and
can reuse versions under the same node ID; use persistent directories for
restart experiments. There is no replica repair or convergence mechanism yet.

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
