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
| **String** | GET, SET, DEL, EXPIRE, TTL | Simple key-value caching |
| **Hash** | HSET, HGET, HDEL, HINCRBY, HGETALL, HKEYS, HVALS | Object/document caching |
| **Set** | SADD, SREM, SMEMBERS, SINTER, SCARD, SISMEMBER | Membership tracking, deduplication |
| **Sorted Set** | ZADD, ZREM, ZRANGE, ZSCORE, ZCARD | Leaderboards, time-series, scoring |
| **List** | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE | Queues, activity feeds, sequences |

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
  --host 0.0.0.0 \
  --port 6379 \
  --max-tenants 1000
```

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed configuration options.

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
