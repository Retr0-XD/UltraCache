# UltraCache v0.1 Issue-Ready Chunks

Use these as GitHub issues. Each item includes a scope, tasks, and exit criteria.

---

## Issue 1: Define MVP spec and invariants
**Scope:** Document v0.1 goals, non-goals, supported commands, and isolation guarantees.

**Tasks**
- Write MVP spec with command subset.
- Define tenant isolation guarantees (memory, CPU, latency).
- Document RESP subset and tenant identity rules.

**Exit criteria**
- MVP spec approved and published in docs.

---

## Issue 2: Shard-per-core runtime skeleton
**Scope:** Create the core runtime with shard-per-core execution and routing.

**Tasks**
- Implement shard threading model (one shard per core).
- Implement shard router (hash-based key routing).
- Add minimal in-memory store per shard.

**Exit criteria**
- Server can accept multiple connections and route keys deterministically.

---

## Issue 3: RESP subset + networking layer
**Scope:** Build RESP parsing and command dispatch for core commands.

**Tasks**
- Implement RESP parser/serializer (subset).
- Add connection lifecycle handling.
- Implement `PING`, `GET`, `SET`.

**Exit criteria**
- Client can connect and execute basic commands successfully.

---

## Issue 4: Tenant registry + identity resolution
**Scope:** Introduce tenant registry and request-level tenant resolution.

**Tasks**
- Implement tenant config schema.
- Build tenant registry (create/update/delete).
- Resolve tenant via `AUTH` token or key prefix.

**Exit criteria**
- Requests are scoped to tenant identity end-to-end.

---

## Issue 5: Per-tenant memory accounting
**Scope:** Track memory usage per tenant with hard caps.

**Tasks**
- Implement per-tenant memory accounting for entries.
- Enforce hard memory cap per tenant.
- Return errors or evict on limit exceedance.

**Exit criteria**
- Tenant cannot exceed memory limit under stress.

---

## Issue 6: Per-tenant LRU eviction
**Scope:** Add per-tenant LRU eviction policy.

**Tasks**
- Implement LRU per tenant.
- Integrate eviction on memory pressure.
- Validate eviction isolation.

**Exit criteria**
- Eviction affects only the owning tenant.

---

## Issue 7: TTL and expiration
**Scope:** Implement TTL scheduling and expiry cleanup.

**Tasks**
- Add `EXPIRE`, `TTL` support.
- Implement expiration scheduling per tenant.
- Ensure expired keys are removed correctly.

**Exit criteria**
- TTL works across tenants without cross-impact.

---

## Issue 8: CPU accounting + throttling
**Scope:** Track per-tenant CPU usage and throttle on budget exhaustion.

**Tasks**
- Track per-command execution time per tenant.
- Implement token-bucket throttling per tenant.
- Return throttle errors or delay execution on budget breach.

**Exit criteria**
- Noisy tenant throttled while others remain unaffected.

---

## Issue 9: Latency tracking (p99)
**Scope:** Track per-tenant p99 latency with a rolling window.

**Tasks**
- Implement rolling window latency tracking.
- Expose per-tenant p99 metric.

**Exit criteria**
- p99 latency visible per tenant.

---

## Issue 10: Data types (Hash/Set/ZSet)
**Scope:** Add v0.1 data type support beyond strings.

**Tasks**
- Implement Hash commands: `HGET`, `HSET`, `HDEL`.
- Implement Set commands: `SADD`, `SREM`, `SMEMBERS`.
- Implement ZSet commands: `ZADD`, `ZREM`, `ZRANGE`.

**Exit criteria**
- All commands pass correctness tests.

---

## Issue 11: Observability + admin stats
**Scope:** Add metrics and basic admin endpoint.

**Tasks**
- Expose per-tenant metrics (memory, CPU, latency, evictions).
- Add admin endpoint to list tenants and quotas.
- Add structured logs for command execution.

**Exit criteria**
- Metrics available and admin endpoint returns tenant stats.

---

## Issue 12: Noisy-neighbor benchmark harness
**Scope:** Build a benchmark to validate isolation.

**Tasks**
- Implement load generator (multi-tenant).
- Add scenarios for noisy neighbor tests.
- Document benchmark results.

**Exit criteria**
- Benchmark demonstrates isolation under load.
