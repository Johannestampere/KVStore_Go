# kvstore

A distributed key-value store in Go using only the standard library. It combines
consistent hashing, configurable replication and quorums, durable local storage,
Prometheus metrics, and a concurrent benchmark CLI.

The default cluster has five nodes, three replicas per key, and read/write
quorums of two. Any node can coordinate requests. One unavailable replica does
not prevent operations while the required quorum remains reachable.

## Quick start

Requires **Go 1.27+**. Persistent storage supports macOS and Linux local
filesystems. Docker Engine and Compose are needed only for the container demo.

Start a persistent standalone node on `127.0.0.1:8001`:

```bash
go run ./cmd/node -data-dir data/standalone
```

In another terminal:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/user:42 \
  -H 'Content-Type: application/json' -d '{"value":"Alice"}'
curl -i http://127.0.0.1:8001/kv/user:42
curl -i -X DELETE http://127.0.0.1:8001/kv/user:42
curl -i http://127.0.0.1:8001/kv/user:42
```

Expect `204`, `200` with `{"key":"user:42","value":"Alice"}`, `204`, then `404`.
Restart with the same data directory to recover saved records and deletions.
Omit `-data-dir` for memory-only storage. Use `go build ./cmd/node` to build a
standalone executable.

## Five-node cluster

Stop other processes using ports 8001–8005, then run:

```bash
docker compose up --build -d --wait
docker compose ps
```

Nodes `node-a` through `node-e` expose those ports on host loopback. Containers
communicate on port 8000 using service names from
[cluster.docker.json](configs/cluster.docker.json). Each node runs as a non-root
user with its own persistent volume. Health probes run every five seconds.

`docker compose down` retains volumes. Adding `--volumes` deletes stored data.
Health failures neither restart containers automatically nor change membership.

For native processes, [cluster.local.json](configs/cluster.local.json) defines
the same five node IDs on ports 8001–8005. Run this in one terminal per node,
changing the ID, port, and storage directory for each:

```bash
go run ./cmd/node -config configs/cluster.local.json \
  -id node-a -addr 127.0.0.1:8001 -data-dir data/node-a
```

| Node flag | Purpose | Default |
| --- | --- | --- |
| `-addr` | Listening address | `127.0.0.1:8001` |
| `-config`, `-id` | Shared cluster file and local identity; supplied together | Standalone mode |
| `-data-dir` | Exclusive persistent directory | Memory only |
| `-peer-timeout` | Deadline per outgoing peer request | `2s` |
| `-request-timeout` | Coordinated operation deadline; positive and below 10s | `5s` |

All nodes must agree on membership, virtual-node count, replication factor, and
quorums. Configuration is validated at startup and requires a restart to reload.
Replication defaults to three copies; both quorums default to a majority.
Settings must satisfy `1 ≤ R,W ≤ N ≤ member count` and `R + W > N`.
The sample ring uses 64 virtual positions per physical node.

## HTTP API

| Request | Success | Important errors |
| --- | --- | --- |
| `PUT /kv/{key}` | `204` | `400` invalid JSON, `413` oversized body, `415` unsupported content type |
| `GET /kv/{key}` | `200` with JSON key/value | `404` missing or deleted |
| `DELETE /kv/{key}` | `204`, including absent keys | Uses the same quorum requirements as PUT |
| `GET /health` | `200` with `{"status":"ok"}` | Process liveness only; does not check disk or quorum |
| `GET /metrics` | `200` with Prometheus text | Node-local measurements; does not require quorum |

PUT requires `application/json` and exactly one object containing a string
`value`. Empty values are valid; missing/null values, unknown fields, and trailing
JSON are rejected. Bodies are limited to 1 MiB including JSON overhead. Keys
must be valid UTF-8, at most 4 KiB, and occupy one non-empty URL segment. Escape
reserved characters: `user/42` becomes `/kv/user%2F42`; dot-only keys need
`%2E` or `%2E%2E` to avoid path cleaning.

Cluster operations may return `503` for unavailable quorum, `504` for deadlines,
`502` for malformed peer responses, or `409` for version conflicts. Handler
errors use `{"error":"message"}`; unmatched routes use Go's standard router
responses. GET routes support HEAD. Health and metrics accept GET/HEAD only.

Cluster-only `GET` and `PUT /internal/records/{key}` inspect or apply complete
local records without forwarding. Missing records return `404`; tombstones
return `200`. Records contain `key`, `value`, `version`, and `deleted`. Internal
bodies are limited to 8 MiB; decoded values to 1 MiB. These endpoints are for
trusted local experiments: there is no authentication or TLS termination.

## Architecture and consistency

```text
Client → any node → Coordinator → consistent-hash replica set
                                  /        |        \
                               Node A    Node C    Node E
                                  local storage or HTTP
```

The immutable ring hashes keys and virtual positions with SHA-256, uses binary
search, wraps clockwise, and skips duplicate physical owners. Every node uses
the same placement rules. Membership is static; changing configuration does not
migrate data.

**PUT/DELETE:** read a quorum to observe versions, reserve a higher logical
version locally, then send the same record to all assigned replicas concurrently.
Return when enough replicas acknowledge. DELETE writes a versioned deletion
marker, or *tombstone*. Outstanding writes continue after success, bounded by
the original operation deadline.

**GET:** contact all replicas and return the highest version among the first
`R` valid responses. Missing-key responses count toward quorum. A winning
tombstone yields `404`; conflicting contents with an identical version yield a
conflict. Remaining reads are canceled after quorum completes.

Versions compare an unsigned counter first, then node ID lexically to break
ties. Each store tracks one counter across keys; incoming records advance it.
Older records are rejected, identical retries are accepted without changes,
and counter overflow fails explicitly. Reserving a version does not store an
extra record on a coordinator outside the replica set.

Goroutines execute replica calls concurrently; buffered channels collect their
results. Cancellation and deadlines bound network work. Quorum collection stops
when enough calls succeed or too few remaining calls could succeed. There are
no automatic retries or substitute replicas.

**Guarantees and limits:** quorum overlap applies within the unchanged replica
set, but does not establish linearizability. Concurrent or partial writes can
produce different observations across read quorums. Failed, timed-out, or
canceled writes may still take effect; there is no rollback. Reads do not repair
replicas, and there is no automatic convergence after an outage. Transactions,
consensus, dynamic discovery, and live rebalancing are outside the current scope.

## Persistence and shutdown

Each persistent node owns an append-only log bound to its identity. An advisory
lock prevents concurrent writers to the same directory. A single writer
serializes mutations, appends an entry, calls `File.Sync`, then publishes the
change in memory. Reads continue against the last published state during disk
I/O. Version reservations are also synchronized, even when the coordinator
stores no replica of the value.

Startup replays records, tombstones, and counters before opening the listener.
Frames use length and CRC32 checksums for headers and payloads. An incomplete
final frame is truncated; complete corruption or invalid ordering fails startup.
After an append/sync error, writes fail until reopening; a failed write may still
appear after recovery. Reads retain the last successfully published state.

SIGINT/SIGTERM stops admission, drains HTTP requests, drains replication, then
closes storage. HTTP and replication draining each have a ten-second budget.
An admitted disk operation finishes before storage closes; filesystem operations
are not context-cancellable and may outlast those budgets.

There is no compaction or snapshotting: historical log entries accumulate,
tombstones remain in memory, and replay cost grows. `File.Sync` requests OS-level
synchronization; this is not a production-grade power-loss durability guarantee.
Memory-only restarts lose records and counters and can reuse versions under the
same node ID. Use persistent directories for restart experiments.

## Failure and restart demo

In the supplied topology, `node-c` owns a replica of both demo keys:

```bash
curl -i -X PUT http://127.0.0.1:8001/kv/demo:persist \
  -H 'Content-Type: application/json' -d '{"value":"survives restart"}'
curl -i -X PUT http://127.0.0.1:8002/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"before failure"}'
curl -i http://127.0.0.1:8003/internal/records/demo:persist
```

Confirm the last response is `200` before stopping the replica; retry if its
background write has not finished. Then:

```bash
docker compose stop node-c
curl -i http://127.0.0.1:8002/kv/demo:quorum
curl -i -X PUT http://127.0.0.1:8002/kv/demo:quorum \
  -H 'Content-Type: application/json' -d '{"value":"during failure"}'
curl -i http://127.0.0.1:8002/kv/demo:quorum
docker compose up -d --wait node-c
curl -i http://127.0.0.1:8003/internal/records/demo:persist
```

The surviving nodes serve reads and writes; restart restores `survives restart`
from local storage. It does not recover updates the stopped node missed.
Stopping two replicas of the same key prevents quorum and produces `503` or
`504`. Keep membership unchanged throughout the experiment.

## Metrics

```bash
curl -s http://127.0.0.1:8001/metrics
```

| Metric | Type | Labels |
| --- | --- | --- |
| `kvstore_http_requests_total` | Counter | `scope`, `method`, `status` |
| `kvstore_http_request_duration_seconds` | Histogram | `scope`, `method` |
| `kvstore_replica_failures_total` | Counter | `operation` |
| `kvstore_quorum_failures_total` | Counter | `phase` |

Measurements reset on restart. Public and internal HTTP traffic are separated;
health probes and scrapes are excluded. Labels never include individual keys or
URLs. HTTP latency measures completed handler calls, including errors, but
excludes background replication. It is not client round-trip latency.

Replica counters include failed local/remote attempts and background writes;
valid missing keys and explicit cancellations do not count. Quorum counters
identify read/write phases, so a PUT can fail during its initial read phase.
Deadlines count as failures. A conflict discovered after obtaining a read quorum
does not count as failure to reach quorum.

No monitoring server is bundled. A host-running Prometheus can scrape all nodes:

```yaml
scrape_configs:
  - job_name: kvstore
    scrape_interval: 5s
    static_configs:
      - targets: ['127.0.0.1:8001', '127.0.0.1:8002', '127.0.0.1:8003', '127.0.0.1:8004', '127.0.0.1:8005']
```

Use `sum(rate(kvstore_http_requests_total{scope="public"}[5m]))` for request rate.
Histogram percentiles are bucket estimates. There is no peer-health polling,
`healthy_nodes` metric, or automatic change to ownership when a node fails.

## Benchmarks

```bash
go run ./cmd/bench \
  -nodes http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003,http://127.0.0.1:8004,http://127.0.0.1:8005 \
  -requests 5000 -concurrency 20 -read-percent 50 \
  -keys 1000 -value-bytes 256 -timeout 5s -format json \
  -description '5 nodes; N=3 R=2 W=2; persistent storage; record hardware'
```

Use `-read-percent 100` for GET-only or `0` for PUT-only; omit `-format json`
for text. Read keys are preloaded outside measurement and their returned values
are checked. PUTs insert distinct keys. Workers rotate entry nodes independently
per method; reads and writes use separate namespaces. Generated keys remain in
storage, so use a disposable cluster. This is not a hot-key or concurrent-update
workload.

Reports include successful operations/second, error rate, status counts, and
nearest-rank p50/p95/p99 latency, overall and per method. Successful-only and
all-attempt latency are separate. Timing includes response reading and validation;
preload and reporting are excluded. Ctrl-C produces partial results. Preparation
errors, cancellation, or measured failures cause a nonzero exit.

This fixed-concurrency, closed-loop client sends another request only after a
response. Offered load drops during stalls, so it can underrepresent overload
latency through coordinated omission. There is no separate steady-state warmup;
preload warms read workloads, while PUT-only runs start with cold connections.
Record hardware, topology, quorums, storage, and deadlines alongside results.

[Six local runs](docs/benchmarks/local-2026-10-08.md) used an Apple M5 Pro with
24 GiB RAM, persistent native nodes, and the workload above:

| Condition | Median successful ops/s | Measured requests | Errors |
| --- | ---: | ---: | ---: |
| Five nodes running | 248.41 | 15,000 | 0 |
| One replica stopped | 244.38 | 15,000 | 0 |

Each condition had three runs. These short, shared-host measurements demonstrate
behavior, not production capacity. The report includes per-method latency,
reproduction steps, and [raw JSON](docs/benchmarks/local-2026-10-08.json).
For replica-failure comparisons, exclude the stopped node from client entry URLs
while retaining it in cluster membership.

## Development

```bash
go vet ./...
go test -race ./...
docker compose config --quiet
```

Tests cover placement, versioning, concurrent requests, quorum failures, timeouts,
background replication, corruption recovery, crash/restart behavior, metrics,
and benchmark reporting. Go tests do not require Docker; race detection needs a
C compiler. The Docker demo separately exercises container networking and volumes.

| Location | Responsibility |
| --- | --- |
| `cmd/node`, `cmd/bench` | Node server and load-testing CLI |
| `internal/api`, `internal/transport` | Public/internal handlers and peer HTTP client |
| `internal/cluster`, `internal/config`, `internal/consistenthash` | Membership, configuration, and placement |
| `internal/replication`, `internal/routing` | Quorum coordination and standalone adapter |
| `internal/storage` | Versioned memory store, journal, and recovery |
| `internal/metrics`, `internal/benchmark` | Instrumentation and workload measurements |
| `tests/unit`, `tests/integration` | Component and end-to-end checks |
| `configs`, `docs/benchmarks` | Sample topologies and measured results |
