# Multi-Tenancy Guide

UltraCache's core feature is first-class multi-tenant support. This guide covers tenant isolation, resource management, and best practices.

## Concepts

### What is a Tenant?

A tenant represents a logical user, application, or team within UltraCache. Each tenant:

- Has its own isolated key-value store
- Cannot see or access other tenants' data
- Has resource budgets (memory, CPU)
- Can have independent TTL policies

### Default Tenant

If no tenant is specified, UltraCache uses the `default` tenant. All operations are initially scoped to this tenant.

---

## Authentication

### Switching Tenants

Use the `AUTH` command with a tenant token:

```bash
AUTH my-tenant-id

# All subsequent operations are scoped to this tenant
SET key "value"
GET key
# Returns: "value"

# Switch to another tenant
AUTH other-tenant-id

# This tenant has separate data
GET key
# Returns: (nil)
```

### Tenant Token Format

Tenant tokens are simple strings. You can use:
- Tenant names: `"tenant-a"`, `"production"`, `"staging"`
- UUIDs: `"550e8400-e29b-41d4-a716-446655440000"`
- Application IDs: `"app-123-456"`

Example:

```bash
AUTH tenant-development
AUTH 550e8400-e29b-41d4-a716-446655440000
AUTH app:backend:prod
```

---

## Data Isolation

### Complete Separation

Data is completely isolated between tenants:

```python
import redis

client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

# Tenant A
client.execute_command('AUTH', 'tenant-a')
client.set('user:1:name', 'Alice')
client.set('user:1:email', 'alice@example.com')
client.hset('user:1', mapping={'age': 30})

# Tenant B
client.execute_command('AUTH', 'tenant-b')
client.set('user:1:name', 'Bob')
client.hset('user:1', mapping={'age': 25})

# Tenant C (empty)
client.execute_command('AUTH', 'tenant-c')
print(client.get('user:1:name'))  # Output: None

# Back to A
client.execute_command('AUTH', 'tenant-a')
print(client.get('user:1:name'))  # Output: Alice
print(client.hget('user:1', 'age'))  # Output: 30
```

### No Cross-Tenant Operations

Operations cannot access data from other tenants:

```bash
AUTH tenant-a
SET mykey "from-a"

AUTH tenant-b
# Cannot access tenant-a's data
GET mykey
# Output: (nil)

# DEL and other commands only affect current tenant
DEL mykey  # Doesn't affect tenant-a's data
```

---

## Resource Budgets

### Memory Limits

Each tenant has a memory budget. Default is 64MB per tenant.

**Set memory limit:**
```bash
AUTH my-tenant

# Set to 100MB
MEMORY-LIMIT 104857600

# Set to 500MB
MEMORY-LIMIT 524288000
```

**What happens when limit is exceeded:**

1. LRU (Least Recently Used) eviction occurs automatically
2. Oldest unused keys are removed to stay within budget
3. Operations fail if no eviction can free enough space

**Example:**
```bash
AUTH tenant-limited
MEMORY-LIMIT 1024  # Only 1KB!

SET key1 "value1"  # OK
SET key2 "value2"  # OK (may evict key1)
GET key1           # Might return (nil) if evicted
```

### CPU Quotas

Each tenant has a CPU execution quota. Default is 5000 microseconds per second.

**Set CPU quota:**
```bash
AUTH my-tenant

# Set to 10ms per second
CPU-THROTTLE 10000

# Set to 100ms per second
CPU-THROTTLE 100000
```

**What happens when quota is exceeded:**

1. Slow commands are throttled
2. Concurrent requests may be delayed
3. Ensures one tenant cannot monopolize CPU

---

## Tenant Listing and Monitoring

### List All Tenants

```bash
TENANTS
```

Returns information about all registered tenants:

```
id=default memory_limit_bytes=67108864 cpu_quota_micros=5000
id=tenant-a memory_limit_bytes=67108864 cpu_quota_micros=5000
id=tenant-b memory_limit_bytes=104857600 cpu_quota_micros=10000
```

### Check Current Tenant Stats

```bash
AUTH my-tenant
STATS
```

Returns detailed statistics for the current tenant:
- Number of keys
- Memory used
- Operation counts
- Eviction events
- CPU time used

---

## Best Practices

### 1. Use Meaningful Tenant IDs

```bash
# Good - descriptive
AUTH app:payment:production
AUTH team:analytics:staging
AUTH customer:acme:prod

# Avoid - vague
AUTH t1
AUTH x
```

### 2. Set Appropriate Resource Limits

```bash
# Critical service - generous limits
AUTH critical-cache
MEMORY-LIMIT 1073741824      # 1GB
CPU-THROTTLE 100000          # 100ms/sec

# Development tenant - modest limits
AUTH dev-cache
MEMORY-LIMIT 134217728       # 128MB
CPU-THROTTLE 10000           # 10ms/sec

# Testing tenant - minimal limits
AUTH test-cache
MEMORY-LIMIT 67108864        # 64MB
CPU-THROTTLE 5000            # 5ms/sec
```

### 3. Monitor Tenant Health

Periodically check tenant stats:

```bash
for tenant in $(redis-cli TENANTS); do
  redis-cli AUTH "$tenant"
  echo "Stats for $tenant:"
  redis-cli STATS
  echo
done
```

### 4. Handle Eviction Gracefully

Design applications to handle key evictions:

```python
def get_cached_value(key, default=None):
    value = client.get(key)
    if value is None:
        # Key might have been evicted - reload from source
        value = load_from_database(key)
        client.set(key, value, ex=3600)
    return value
```

### 5. Use Appropriate TTLs

Set TTL on temporary data:

```bash
# Session cache - expires in 1 hour
SET session:abc123 "{...}" 
EXPIRE session:abc123 3600

# Leaderboard - persistent
ZADD leaderboard 1000 "alice"
# No expiration
```

### 6. Isolate Tenant Operations

Keep tenant switching minimal:

```python
# Good - operate within same tenant
client.execute_command('AUTH', 'tenant-a')
for i in range(1000):
    client.set(f'key:{i}', f'value:{i}')

# Avoid - frequent switching
for i in range(1000):
    client.execute_command('AUTH', f'tenant-{i}')
    client.set('key', 'value')
```

---

## Security Considerations

### Tenant Tokens

- Tenant tokens are the only authentication mechanism
- There is no password protection - tokens are secrets
- Store tokens securely (environment variables, secrets manager)
- Rotate tokens periodically

### Data Isolation

- Data is logically isolated within a single UltraCache process
- No cryptographic guarantees (not encrypted at rest)
- Use separate UltraCache instances for strict security requirements

### Network Access

- UltraCache listens on a network port - restrict access with firewall rules
- Use TLS/SSL at the infrastructure level (load balancer, VPN, etc.)
- Don't expose UltraCache directly to untrusted networks

---

## Scaling Multi-Tenancy

### Horizontal Scaling

Run multiple UltraCache instances:

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  UltraCache-1   │      │  UltraCache-2   │      │  UltraCache-3   │
│  Port: 6379     │      │  Port: 6380     │      │  Port: 6381     │
└─────────────────┘      └─────────────────┘      └─────────────────┘
 tenant-a, b, c      tenant-d, e, f           tenant-g, h, i
```

**Strategy:** Assign tenant ranges to specific instances.

### Load Balancing

Route tenants to appropriate instances:

```python
def get_cache_connection(tenant_id):
    # Hash tenant to instance
    instance_num = hash(tenant_id) % 3
    return redis.Redis(
        host=f'ultracache-{instance_num}',
        port=6379
    )
```

### Monitoring Tenants

Track resource usage per tenant:

```bash
# Get memory used by tenant
redis-cli AUTH tenant-a STATS | grep "used_bytes"

# Check eviction rate
redis-cli AUTH tenant-a STATS | grep "evictions"

# Monitor CPU usage
redis-cli AUTH tenant-a STATS | grep "cpu"
```

---

## Troubleshooting

### Tenant Data Missing

**Symptom:** Expected key returns (nil) after switching tenants

**Cause:** Operating on different tenant or key was evicted

**Solution:**
```bash
# Verify current tenant
TENANTS  # Check which tenant is active
AUTH correct-tenant
GET key

# If still missing, check if evicted
# Reload from source and re-set
```

### Memory Limit Exceeded

**Symptom:** Operations start failing with memory errors

**Cause:** Tenant exceeded memory budget

**Solution:**
```bash
# Check current memory
STATS  # Look for "used_bytes"

# Increase limit
MEMORY-LIMIT 209715200  # Increase to 200MB

# Or optimize by removing unused keys
FLUSHDB  # Clear entire cache
```

### Slow Performance on One Tenant

**Symptom:** One tenant is slow while others are fast

**Cause:** CPU quota exceeded or resource contention

**Solution:**
```bash
# Check CPU usage
STATS  # Look for "cpu_time_micros"

# Increase CPU quota
CPU-THROTTLE 50000  # Increase to 50ms/sec

# Or reduce load on that tenant
```
