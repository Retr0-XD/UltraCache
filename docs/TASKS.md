# UltraCache v0.1 Task List

This list mirrors the roadmap milestones and can be converted into issues.

## 1) Define MVP scope and invariants
- [x] Draft MVP spec (supported commands, limits, non-goals)
- [x] Define tenant isolation guarantees (memory, CPU, latency)
- [x] Decide RESP subset and tenant identity rules

## 2) Core architecture decisions
- [x] Specify shard-per-core + actor model details
- [x] Define shard routing strategy (hashing, tenant-aware distribution)
- [x] Define data model layout per shard (tenant → keyspace → entry)

## 3) Tenant system
- [x] Design tenant config schema
- [x] Implement tenant registry (create/update/delete)
- [x] Implement tenant identity resolution (auth token or prefix)

## 4) Memory management
- [x] Build per-tenant memory pools with hard caps
- [x] Implement per-tenant LRU eviction
- [x] Add memory accounting per entry (key + value + metadata)

## 5) CPU & latency isolation
- [x] Implement per-tenant CPU usage tracking
- [x] Add throttling/backpressure when limits exceeded
- [ ] Track per-tenant p99 latency (rolling window)

## 6) Data structures (v0.1 subset)
- [x] Implement String data type + commands ✅
- [x] Implement Hash data type + commands ✅
- [x] Implement Set data type + commands ✅
- [x] Implement Sorted Set data type + commands ✅
- [x] Add TTL/expiration scheduling per tenant ✅

## 7) Networking & protocol
- [x] Implement RESP parser/serializer (subset)
- [x] Build command router (tenant-aware, shard-aware)
- [x] Implement connection lifecycle (auth, tenant resolution)

## 8) Persistence (optional v0.1-lite)
- [ ] Implement append-only log per tenant
- [ ] Define recovery flow (replay on startup)

## 9) Observability & ops
- [ ] Expose metrics (per-tenant memory, CPU, latency, evictions)
- [ ] Add admin endpoint (list tenants, quotas, stats)
- [ ] Add structured logging for command execution

## 10) Testing & benchmarking
- [x] Unit tests for isolation boundaries ✅
- [x] Correctness tests for data types & TTL ✅
- [ ] Load tests for noisy-neighbor scenarios
