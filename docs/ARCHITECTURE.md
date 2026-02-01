# Architecture

UltraCache's internal design and architecture overview.

## High-Level Design

```
┌──────────────────────────────────────────────────┐
│            RESP Protocol Handler                 │
│         (Tokio Async Runtime)                    │
└────────────┬─────────────────────────────────────┘
             │ Routes to shard
             │
    ┌────────┴────────┬─────────────┬──────────────┐
    │                 │             │              │
┌───▼───┐        ┌───▼───┐    ┌───▼───┐      ┌───▼───┐
│Shard0 │        │Shard1 │    │Shard2 │ ...  │Shard15│
└───┬───┘        └───┬───┘    └───┬───┘      └───┬───┘
    │                │            │              │
┌───▼───────────────▼────────────▼──────────────▼────┐
│          Per-Shard LRU Caches                      │
│   + Tenant State Tracking                         │
│   + Memory Management                             │
└──────────────────────────────────────────────────┘
    │
    └──────────────────────────┬──────────────────────┐
                               │                      │
                       ┌──────▼──────┐        ┌──────▼──────┐
                       │  AOF Logger  │        │  Eviction   │
                       │  Per-Tenant  │        │  Policy     │
                       └──────────────┘        └─────────────┘
```

---

## Core Components

### RESP Protocol Handler

Handles the Redis Serialization Protocol (RESP):

- Parses command frames
- Routes commands to appropriate shard
- Serializes responses
- Manages connections

**Location:** `src/main.rs`

### Shard Runtime

16 independent shards for concurrent processing:

- Each shard is a separate execution context
- Reduces lock contention
- Enables multi-core utilization
- Per-shard LRU cache

**Location:** `src/runtime.rs`

### Entry Data Structure

Represents cached values:

```rust
enum EntryData {
    String(Vec<u8>),
    Hash(HashMap<String, Vec<u8>>),
    Set(HashSet<String>),
    ZSet(BTreeMap<String, f64>),
    List(VecDeque<Vec<u8>>),
}

struct Entry {
    data: EntryData,
    expires_at: Option<SystemTime>,
    last_accessed: SystemTime,
    size_bytes: u64,
}
```

### Tenant State

Per-tenant metadata and resource tracking:

```rust
struct TenantState {
    cache: LruCache<String, Entry>,
    used_bytes: u64,
    tenant_limit_bytes: u64,
    cpu_quota_micros: u64,
    stats: TenantStats,
}
```

### AOF Persistence

Append-Only File with per-tenant logging:

```rust
pub struct AofManager {
    base_dir: PathBuf,
    fsync_policy: FsyncPolicy,
    files: HashMap<String, BufWriter<File>>,
    last_fsync: HashMap<String, SystemTime>,
}
```

**Fsync policies:**
- `Always` - fsync after each write
- `EverySecond` - fsync once per second
- `No` - let OS handle fsync

---

## Data Structures

### String Type

```rust
// Stored as: Vec<u8>
String("hello".as_bytes().to_vec())
```

Simple byte vector for key-value pairs.

### Hash Type

```rust
// Stored as: HashMap<String, Vec<u8>>
Hash(vec![
    ("name".to_string(), "Alice".as_bytes().to_vec()),
    ("email".to_string(), "alice@example.com".as_bytes().to_vec()),
].into_iter().collect())
```

Field-value pairs indexed by field name.

### Set Type

```rust
// Stored as: HashSet<String>
Set(vec!["redis", "cache", "database"].into_iter().collect())
```

Unique unordered members.

### Sorted Set Type

```rust
// Stored as: BTreeMap<String, f64>
// Ordered by score (key=member, value=score)
ZSet(vec![
    ("alice".to_string(), 100.0),
    ("bob".to_string(), 200.0),
].into_iter().collect())
```

Members with floating-point scores, ordered by score.

### List Type

```rust
// Stored as: VecDeque<Vec<u8>>
List(VecDeque::from(vec![
    "first".as_bytes().to_vec(),
    "second".as_bytes().to_vec(),
]))
```

Doubly-linked queue for efficient head/tail operations.

---

## Execution Flow

### Command Execution

1. Client sends RESP-formatted command
2. Handler parses the command
3. Command is routed to a shard based on key hash
4. Shard acquires tenant state lock
5. Command is executed
6. Response is serialized and sent
7. AOF is updated (if persistence enabled)

### Shard Selection

```rust
// Hash-based routing to one of 16 shards
let shard_id = hash(&key) % 16;
```

### Lock-Free Read Optimization

Reads use RwLock to allow concurrent access:

```rust
let read_guard = shard.read();  // Multiple readers allowed
let value = read_guard.get(&key);
drop(read_guard);
```

Writes use exclusive locks:

```rust
let mut write_guard = shard.write();  // Single writer
write_guard.insert(key, value);
```

---

## Memory Management

### LRU Eviction

Each shard maintains an LRU cache:

```rust
// When memory limit exceeded:
// 1. Find least-recently-used entry
// 2. Delete it
// 3. Continue until under limit
```

**Eviction trigger:** When `used_bytes > tenant_limit_bytes`

### Entry Size Calculation

Sizes are tracked accurately:

```rust
fn calculate_entry_size(data: &EntryData) -> u64 {
    match data {
        String(v) => v.len() as u64,
        Hash(m) => m.keys().map(|k| k.len()).sum::<usize>() as u64 +
                   m.values().map(|v| v.len()).sum::<usize>() as u64,
        // ... other types
    }
}
```

### Tenant Isolation

Each tenant has:
- Independent LRU cache per shard
- Independent `used_bytes` counter
- Independent `tenant_limit_bytes` quota

**Result:** Complete resource isolation.

---

## TTL and Expiration

### Expiration Check

Done lazily on access:

```rust
fn is_expired(entry: &Entry) -> bool {
    match entry.expires_at {
        Some(time) => SystemTime::now() > time,
        None => false,
    }
}
```

When expired:
- Entry is deleted
- Caller receives nil/empty response
- Memory is freed

### No Background Cleanup

Expired entries are cleaned on access (lazy expiration):

**Advantage:** Minimal CPU overhead
**Disadvantage:** Disk space might grow

---

## Command Routing

Commands are categorized:

```rust
enum Command {
    // String
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    
    // Hash
    Hget { key: String, field: String },
    Hset { key: String, field: String, value: Vec<u8> },
    
    // ... other commands
}
```

Each command variant is routed to appropriate shard:

```rust
match cmd {
    Command::Get { key } => {
        let shard_id = hash(&key) % SHARD_COUNT;
        execute_on_shard(shard_id, cmd)
    }
    // ... other commands
}
```

---

## Concurrency Model

### Multi-Threaded Tokio Runtime

UltraCache uses Tokio's work-stealing scheduler:

```
┌────────────────┐    ┌────────────────┐    ┌────────────────┐
│    Thread 1    │    │    Thread 2    │    │    Thread N    │
│   Tokio Work   │    │   Tokio Work   │    │   Tokio Work   │
│    Stealing    │    │    Stealing    │    │    Stealing    │
└────────────────┘    └────────────────┘    └────────────────┘
         │                   │                       │
         └───────────────────┴───────────────────────┘
                  Shared Work Queue
```

### Shard-Level Synchronization

Each shard is protected by RwLock:

```rust
Arc<RwLock<ShardData>>
```

- Multiple readers (GET operations)
- Single writer (SET/DELETE operations)
- Lock-free for read-heavy workloads

### Tenant Isolation Guarantee

Tenant isolation is enforced at multiple levels:

1. **Logical:** Each tenant has separate LRU cache
2. **Operational:** Operations are scoped to current tenant
3. **Memory:** Independent `used_bytes` and `tenant_limit_bytes`

---

## Performance Optimizations

### Sharding

16 shards reduce lock contention:

```
Without sharding:  1 lock for all keys
                   ✓ Simple
                   ✗ High contention
                   
With 16 shards:    16 locks (one per shard)
                   ✓ Parallel execution
                   ✓ Lower contention
```

### LRU Cache

Fast key lookup with eviction:

```rust
// O(1) get and set
// O(1) eviction (tracks access order)
```

### Lazy Expiration

No background cleanup thread:

```
With cleanup thread:  CPU overhead always present
Lazy expiration:      Only pay cost on access
```

### BTreeMap for Sorted Sets

Maintains order efficiently:

```
ZRANGE 0 -1  →  Linear iteration (no sorting needed)
ZSCORE       →  O(log n) lookup
```

---

## Limitations and Design Tradeoffs

### No Cluster Mode

UltraCache is single-instance:

- ✓ Simpler design
- ✓ No network overhead
- ✗ Limited to one machine's resources

**Workaround:** Use external load balancer for multiple instances.

### No Replication

No built-in master-replica:

- ✓ Simpler code
- ✓ Consistent writes
- ✗ Single point of failure

**Workaround:** Use persistence (AOF) for recovery.

### Lazy Expiration Only

Expired keys cleaned on access:

- ✓ Zero CPU overhead
- ✓ Predictable latency
- ✗ Disk space might grow

**Workaround:** Set reasonable TTLs.

### No Transactions

Commands are atomic but no multi-command transactions:

- ✓ Simpler implementation
- ✓ Better concurrency
- ✗ Cannot GROUP operations

**Workaround:** Use pipelining (faster than MULTI/EXEC anyway).

---

## Future Optimizations

Potential improvements:

1. **Cluster Mode** - Distributed sharding across nodes
2. **Replication** - Master-replica for HA
3. **Transactions** - MULTI/EXEC support
4. **Pub/Sub** - Publish-subscribe messaging
5. **Modules** - Custom Lua scripting
6. **Compression** - Value compression for large values
