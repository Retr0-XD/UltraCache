# UltraCache

**A Cloud-Native, Multi-Tenant In-Memory Data Platform**

---

## Overview

Redis is one of the most successful infrastructure systems ever built — but it was designed for a **single-tenant world**.

Modern platforms are different:

* Shared clusters
* Multiple teams
* Multiple workloads
* Strict resource isolation
* Predictable latency guarantees

Today, Redis is still widely used in these environments, but **only by forcing isolation at the infrastructure level** (one Redis per tenant, per team, per workload), which leads to:

* Resource fragmentation
* Operational overhead
* Unpredictable behavior under load
* Inefficient CPU and memory utilization

**UltraCache** introduces a new infrastructure primitive:

> A **multi-tenant, shared, in-memory data platform** with *first-class tenant isolation*.

It is **not a Redis replacement**.
It is a **new category** designed for cloud-native platforms where Redis' assumptions no longer hold.

---

## The Problem

Redis assumes:

* One tenant per instance or cluster
* One trusted workload
* Global memory and eviction policies
* A single execution context

This breaks down in modern environments.

### Real problems teams face today

* One tenant's traffic spikes evict another tenant's hot keys
* A slow command blocks all tenants
* Memory eviction is global and unpredictable
* CPU usage cannot be budgeted per tenant
* Operators spin up dozens of Redis clusters just to isolate workloads

This is **not an operational issue**.
It is a **missing abstraction**.

---

## Core Idea

UltraCache introduces **tenants as a first-class primitive** at the data layer.

Each tenant gets:

* Explicit memory budgets
* CPU execution quotas
* Latency isolation
* Predictable eviction behavior

All tenants share:

* The same process
* The same cluster
* The same operational surface

This enables **safe, efficient, shared in-memory infrastructure**.

---

## What UltraCache Is (and Is Not)

### UltraCache **IS**

* A multi-tenant in-memory data platform
* A shared cache for cloud platforms
* A predictable, isolated execution environment
* A Redis-compatible data layer (subset)

### UltraCache **IS NOT**

* A Redis fork
* A database
* A message broker
* A full Redis replacement

---

## Getting Started

### Quick Start

1. **Build the binary:**
   ```bash
   cargo build --release
   ```

2. **Run the server:**
   ```bash
   ./target/release/ultracache
   ```
   Default listens on `127.0.0.1:6379`

3. **Connect with redis-cli or any Redis-compatible client:**
   ```bash
   redis-cli -p 6379
   ```

### Docker

Build and run the Docker image:

```bash
docker build -t ultracache:latest .
docker run -p 6379:6379 ultracache:latest
```

---

## Documentation

Comprehensive usage documentation is available in the [docs/](docs/) folder:

- **[docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)** - Installation, building, and first steps
- **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)** - Complete command reference
- **[docs/MULTI_TENANCY.md](docs/MULTI_TENANCY.md)** - Multi-tenant isolation and resource management
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** - Production deployment and configuration
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Internal design and data structures

---

## Core Features

### Data Types (5/5)

UltraCache supports the following Redis-compatible data types:

| Type | Operations | Use Case |
|------|-----------|----------|
| **String** | GET, SET, DEL, EXPIRE, TTL, INCR, DECR, INCRBY, DECRBY, APPEND, EXISTS, TYPE, PERSIST, PTTL, MSET, MGET | Simple key-value caching, counters, batched ops |
| **Hash** | HSET, HGET, HDEL, HINCRBY, HGETALL, HKEYS, HVALS | Object/document caching |
| **Set** | SADD, SREM, SMEMBERS, SINTER, SCARD, SISMEMBER, SUNION, SDIFF | Membership tracking, deduplication, set algebra |
| **Sorted Set** | ZADD, ZREM, ZRANGE, ZSCORE, ZCARD, ZRANK, ZREVRANGE | Leaderboards, time-series, scoring |
| **List** | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE | Queues, activity feeds, sequences |
| **Keyspace** | KEYS, FLUSHDB, RENAME | Introspection and key management |

### Multi-Tenancy

* **Per-tenant memory budgets** - Strict isolation, no resource stealing
* **Per-tenant CPU quotas** - Predictable execution
* **Per-tenant TTL policies** - Independent expiration behavior
* **Tenant-aware commands** - AUTH for authentication, TENANTS for listing

### Persistence

* **Append-Only File (AOF)** - Durable operation log per tenant
* **Configurable fsync** - Three durability levels (Always, EverySecond, No)
* **Log compaction** - Automatic rewrite for space efficiency
* **Crash recovery** - Replay AOF on startup

### Verifiable Audit Bridge (UltraCache ↔ StateLedger)

UltraCache can publish an **immutable, verifiable audit trail** of every mutating
command to [StateLedger](https://github.com/Retr0-XD/StateLedger), a durable
hash-chained state ledger. The result is a **verifiable cache**: every
`SET`/`DEL`/`INCR`/etc. can later be proven to have happened, in order, by
querying StateLedger and verifying its hash chain + Merkle root.

* **Best-effort, non-blocking** — a failed emission never blocks or fails the
  cache operation.
* **Optional** — when no endpoint is configured, the bridge is a no-op.
* **Async worker** — events are buffered on an unbounded channel and POSTed by a
  background task using `reqwest` (rustls TLS, no OpenSSL dependency).

Enable it with the `--ledger-url` flag or the `ULTRACACHE_LEDGER_URL` env var:

```bash
./target/release/ultracache \
  --ledger-url http://localhost:8080
```

Each emitted record is a `cache.audit` event with a JSON payload of the form:

```json
{
  "tenant": "acme",
  "command": "SET",
  "key": "user:1",
  "summary": "SET user:1 hello"
}
```

StateLedger appends it to its hash chain, so the audit log is tamper-evident and
Merkle-provable. See the StateLedger README for how to verify records.

### Performance

* **Sub-millisecond latency** - Tokio-based async I/O
* **Sharded architecture** - 16 independent shards for horizontal scalability
* **LRU eviction** - Efficient memory management with configurable limits
* **High throughput** - 500K+ ops/sec under standard load

---

## Architecture

UltraCache is built with a **shard-per-core** architecture:

```
┌─────────────────────────────────┐
│      RESP Protocol Handler      │
│     (Async Tokio Runtime)       │
└──────────┬──────────────────────┘
           │
     ┌─────┴─────┬─────────────┬─────────────┐
     │           │             │             │
  Shard 0     Shard 1      Shard 2  ...  Shard 15
     │           │             │             │
  ┌──┴─┐      ┌──┴─┐        ┌──┴─┐        ┌──┴─┐
  │LRU │      │LRU │        │LRU │        │LRU │
  │    │      │    │        │    │        │    │
  └────┘      └────┘        └────┘        └────┘
```

- **Multiple shards** reduce lock contention
- **Per-shard LRU cache** for independent memory management
- **Per-tenant state tracking** across all shards
- **AOF persistence** with per-tenant log files

---

## Usage Examples

### Basic Operations

```bash
# String operations
SET key1 "hello"
GET key1
DEL key1

# Hash operations
HSET user:1 name "Alice" age 30
HGET user:1 name
HGETALL user:1

# Set operations
SADD team:devs "alice" "bob" "charlie"
SMEMBERS team:devs
SISMEMBER team:devs "alice"

# Sorted set operations
ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
ZRANGE leaderboard 0 -1
ZSCORE leaderboard "bob"

# List operations
LPUSH tasks "task1" "task2"
LRANGE tasks 0 -1
RPOP tasks
```

### Multi-Tenant Operations

```bash
# Authenticate as a tenant
AUTH my-tenant-token

# Operations are now scoped to this tenant
SET counter 0
GET counter

# Switch to a different tenant
AUTH other-tenant-token

# This tenant has separate data
GET counter  # Returns nil (doesn't exist in this tenant)

# List all tenants
TENANTS
```

### TTL and Expiration

```bash
# Set a key with expiration
SET session-id "xyz" 
EXPIRE session-id 3600  # Expire in 1 hour

# Check remaining TTL
TTL session-id

# Delete a key
DEL session-id
```

---

## Testing

Run the test suite:

```bash
# Python integration tests (requires pytest or manual invocation)
python3 tests/test_list.py
python3 tests/test_hash_extended.py
python3 tests/test_extended_ops.py

# Rust unit tests
cargo test --release
```



---

## Performance Benchmarks

Typical performance on standard hardware (single instance):

| Metric | Value |
|--------|-------|
| **Throughput** | 500K+ ops/sec |
| **P50 Latency** | < 100 µs |
| **P99 Latency** | < 1 ms |
| **Memory efficiency** | LRU-based with strict per-tenant budgets |

---

## Configuration

UltraCache can be configured via environment variables and command-line arguments:

```bash
./target/release/ultracache \
  --addr 0.0.0.0:6379 \
  --shards 8 \
  --aof \
  --aof-dir ./data/aof \
  --aof-fsync everysec \
  --ledger-url http://localhost:8080
```

| Flag / Env | Default | Description |
|------------|---------|-------------|
| `--addr` / `ULTRACACHE_ADDR` | `0.0.0.0:6379` | Bind address |
| `--shards` / `ULTRACACHE_SHARDS` | # of CPUs | Number of shards |
| `--config` / `ULTRACACHE_CONFIG` | — | JSON/TOML config file |
| `--aof` / `ULTRACACHE_AOF` | off | Enable AOF persistence |
| `--aof-dir` / `ULTRACACHE_AOF_DIR` | `./data/aof` | AOF directory |
| `--aof-fsync` / `ULTRACACHE_AOF_FSYNC` | `everysec` | `always`/`everysec`/`no` |
| `--ledger-url` / `ULTRACACHE_LEDGER_URL` | — | StateLedger audit endpoint |

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed configuration options.

---

## Analysis: Usability, Security, Flexibility & Roadmap

This section evaluates UltraCache across four axes and outlines where v2 should
go. It is written for operators and architects deciding whether (and how) to
adopt the project.

### Usability

- **Drop-in Redis compatibility** — Any Redis client (redis-cli, Lettuce,
  redis-py, go-redis, Jedis) works unchanged over the RESP protocol.
- **Docker Hub images** — Prebuilt multi-arch images are published to
  `docker.io/retr0xd/ultracache` (amd64 + arm64). Pull and run:
  ```bash
  docker pull retr0xd/ultracache:latest
  docker run -p 6379:6379 retr0xd/ultracache:latest
  ```
- **Compose demo** — The companion [StateLedger](https://github.com/Retr0-XD/StateLedger)
  repo ships `docker-compose.yml` that starts both services with the audit
  bridge wired via `ULTRACACHE_LEDGER_URL=http://stateledger:8080`.
- **Zero-config defaults** — Binds `0.0.0.0:6379` and runs without a config
  file; every knob has a flag and an env-var equivalent.
- **Healthcheck** — The image `HEALTHCHECK` issues a real `PING` over TCP, so
  orchestrators get a genuine readiness signal rather than a process check.

### Security

- **Non-root by default** — The runtime image runs as UID `10001`
  (`ultracache`), reducing blast radius if the process is compromised.
- **No OpenSSL dependency** — The audit bridge uses `reqwest` with `rustls-tls`,
  avoiding the CVE surface of OpenSSL and simplifying patching.
- **Best-effort, fail-open bridge** — A StateLedger outage never blocks or
  fails a cache command; the bridge degrades to a no-op and logs the drop.
  (Operators who need guaranteed audit should monitor bridge health and treat
  StateLedger as a required dependency.)
- **Transport** — RESP is plain TCP. Terminate TLS at a proxy (stunnel, Envoy,
  or a service mesh) in production; do not expose 6379 to untrusted networks.
- **Multi-tenancy isolation** — Per-tenant memory and CPU budgets prevent one
  tenant from starving others; enable `AUTH` so tenants are scoped.

### Flexibility

- **Config layering** — built-in defaults < config file < CLI flags < env vars,
  so the same image adapts to any environment without rebuilds.
- **Optional durability** — AOF can be toggled per deployment (`always`,
  `everysec`, `no`) to trade durability for throughput.
- **Optional audit** — The StateLedger bridge is off unless
  `ULTRACACHE_LEDGER_URL` is set, so single-node caches pay no overhead.
- **Shard tuning** — `--shards` lets you match concurrency to the host's core
  count or a container CPU limit.

### v2 Improvement Spots

1. **Replication & cluster mode** — The single-node design is the main scaling
   ceiling; leader-replica or gossip-based clustering would unlock horizontal
   scale and HA.
2. **TLS-native listener** — First-class TLS (and mTLS) on the RESP port instead
   of relying on an external proxy.
3. **Observability** — Expose Prometheus metrics (ops/sec, hit rate, shard
   latency, bridge drop count) and structured logs for production debugging.
4. **Guaranteed audit mode** — A `fail-closed` bridge option that blocks the
   command (or queues durably) when StateLedger is unreachable, for
   compliance-critical deployments.
5. **Web UI / admin console** — A read-only dashboard for tenants, memory, and
   shard health.
6. **Richer data types** — Streams, bitmaps, and HyperLogLog to broaden
   drop-in compatibility with more Redis workloads.

### Best Use Cases

- **High-throughput caching** — Sub-ms, shard-per-core design suits hot-path
  read/write caching (sessions, feature flags, API responses).
- **Multi-tenant SaaS** — Per-tenant memory/CPU budgets keep noisy neighbors in
  check without separate instances.
- **Verifiable cache / audit** — Paired with StateLedger, every mutation becomes
  a tamper-evident `cache.audit` record for compliance and forensic replay.
- **Edge / single-node** — Small static binary and tiny image footprint make it
  ideal for edge and sidecar deployments where a full Redis cluster is overkill.
- **Drop-in Redis replacement** — When you need Redis semantics without the
  operational weight of a clustered Redis, especially with audit requirements.

---

## Contributing

Contributions are welcome! Areas of interest:

* Performance optimization
* Additional data structures
* Replication support
* Cluster mode
* Monitoring and observability

---

## License

Apache 2.0

---

## Support

For questions, issues, or feedback:
- Open an issue on GitHub
- Check [docs/](docs/) for detailed documentation
- See [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for troubleshooting
