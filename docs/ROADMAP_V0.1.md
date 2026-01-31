# UltraCache v0.1 Roadmap

Date: 2026-01-31

## Objectives
- Prove tenant isolation (memory + CPU + latency) in a shared in-memory service.
- Provide a minimal RESP-compatible interface and limited data types.
- Demonstrate predictable behavior under noisy-neighbor load.

## Non-Goals (v0.1)
- Full Redis compatibility
- Durability-first persistence
- Lua scripting, modules, pub/sub, streams
- Strong cross-shard consistency

## Milestones (4 weeks)

### Week 1 — Core skeleton + protocol
**Outcome:** A running server that accepts RESP and routes to shards.
- Define config and tenant schema.
- Implement RESP parser/serializer subset.
- Build shard-per-core skeleton (threading + routing).
- Implement basic commands: `PING`, `GET`, `SET`.
- Basic in-memory store per shard (no eviction yet).

**Exit criteria**
- Server handles multiple clients.
- Keys route deterministically to shards.

### Week 2 — Tenant isolation (memory + accounting)
**Outcome:** Per-tenant memory caps and isolation.
- Implement tenant registry and identity resolution.
- Add per-tenant memory tracking and hard caps.
- Implement per-tenant LRU eviction.
- Add `DEL`, `EXPIRE`, `TTL`.

**Exit criteria**
- Tenants cannot exceed memory limit.
- Eviction is isolated per tenant.

### Week 3 — CPU & latency controls
**Outcome:** Per-tenant CPU budget and p99 latency tracking.
- Add per-command timing and CPU accounting.
- Implement token-bucket throttling per tenant.
- Track per-tenant p99 latency (rolling window).
- Add minimal admin stats endpoint.

**Exit criteria**
- Noisy tenant throttled without impacting others.
- Metrics show per-tenant CPU + latency.

### Week 4 — Data types + load validation
**Outcome:** Minimal Redis-like feature set + noisy-neighbor demo.
- Add Hash/Set/ZSet primitives.
- Add benchmark harness for noisy-neighbor tests.
- Run isolation tests and document results.
- Tighten error handling and failure behavior.

**Exit criteria**
- Feature-complete MVP command subset.
- Demo shows isolation under load.

## MVP Command Subset
**Strings**: `GET`, `SET`, `DEL`, `EXPIRE`, `TTL`, `PING`

**Hashes**: `HGET`, `HSET`, `HDEL`

**Sets**: `SADD`, `SREM`, `SMEMBERS`

**Sorted Sets**: `ZADD`, `ZREM`, `ZRANGE`

## Tenant Model (v0.1)
- Tenant identity via `AUTH <token>` or `tenant:key` prefix.
- Tenant config includes:
  - `memory_limit`
  - `cpu_quota`
  - `eviction_policy` (LRU)
  - `latency_p99_target` (tracking only)

## Observability
- Per-tenant metrics: memory, CPU time, evictions, p99 latency.
- Admin endpoint for listing tenants and quotas.
