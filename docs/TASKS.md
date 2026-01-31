# UltraCache v0.1 Task List

This list mirrors the roadmap milestones and can be converted into issues.

## 1) Define MVP scope and invariants
- [ ] Draft MVP spec (supported commands, limits, non-goals)
- [ ] Define tenant isolation guarantees (memory, CPU, latency)
- [ ] Decide RESP subset and tenant identity rules

## 2) Core architecture decisions
- [ ] Specify shard-per-core + actor model details
- [ ] Define shard routing strategy (hashing, tenant-aware distribution)
- [ ] Define data model layout per shard (tenant → keyspace → entry)

## 3) Tenant system
- [ ] Design tenant config schema
- [ ] Implement tenant registry (create/update/delete)
- [ ] Implement tenant identity resolution (auth token or prefix)

## 4) Memory management
- [ ] Build per-tenant memory pools with hard caps
- [ ] Implement per-tenant LRU eviction
- [ ] Add memory accounting per entry (key + value + metadata)

## 5) CPU & latency isolation
- [ ] Implement per-tenant CPU usage tracking
- [ ] Add throttling/backpressure when limits exceeded
- [ ] Track per-tenant p99 latency (rolling window)

## 6) Data structures (v0.1 subset)
- [ ] Implement String data type + commands
- [ ] Implement Hash data type + commands
- [ ] Implement Set data type + commands
- [ ] Implement Sorted Set data type + commands
- [ ] Add TTL/expiration scheduling per tenant

## 7) Networking & protocol
- [ ] Implement RESP parser/serializer (subset)
- [ ] Build command router (tenant-aware, shard-aware)
- [ ] Implement connection lifecycle (auth, tenant resolution)

## 8) Persistence (optional v0.1-lite)
- [ ] Implement append-only log per tenant
- [ ] Define recovery flow (replay on startup)

## 9) Observability & ops
- [ ] Expose metrics (per-tenant memory, CPU, latency, evictions)
- [ ] Add admin endpoint (list tenants, quotas, stats)
- [ ] Add structured logging for command execution

## 10) Testing & benchmarking
- [ ] Unit tests for isolation boundaries
- [ ] Correctness tests for data types & TTL
- [ ] Load tests for noisy-neighbor scenarios
