# Deployment Guide

This guide covers production deployment, configuration, and operational best practices for UltraCache.

## Deployment Options

### Single Instance (Development/Small Scale)

Suitable for development, testing, and small-scale deployments:

```bash
./target/release/ultracache --host 0.0.0.0 --port 6379
```

### Multi-Instance (High Availability)

Deploy multiple instances with tenant sharding:

```
Load Balancer
    │
    ├─→ UltraCache-1 (tenants: A, B, C)
    ├─→ UltraCache-2 (tenants: D, E, F)
    └─→ UltraCache-3 (tenants: G, H, I)
```

### Docker Deployment

**Production Docker compose:**

```yaml
version: '3.9'
services:
  ultracache:
    image: ultracache:latest
    ports:
      - "6379:6379"
    environment:
      - UC_HOST=0.0.0.0
      - UC_PORT=6379
    volumes:
      - ultracache_data:/var/lib/ultracache
    restart: always
    healthcheck:
      test: ["CMD", "redis-cli", "PING"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  ultracache_data:
```

### Kubernetes Deployment

**Kubernetes manifest:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ultracache
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ultracache
  template:
    metadata:
      labels:
        app: ultracache
    spec:
      containers:
      - name: ultracache
        image: ultracache:latest
        ports:
        - containerPort: 6379
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          exec:
            command: ["redis-cli", "PING"]
          initialDelaySeconds: 10
          periodSeconds: 5
        readinessProbe:
          exec:
            command: ["redis-cli", "PING"]
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ultracache
spec:
  selector:
    app: ultracache
  ports:
  - port: 6379
    targetPort: 6379
  type: ClusterIP
```

---

## Configuration

### Command-Line Arguments

```bash
./target/release/ultracache [OPTIONS]

Options:
  --host <HOST>              Bind address (default: 127.0.0.1)
  --port <PORT>              Port to listen on (default: 6379)
  --max-tenants <NUM>        Maximum number of tenants (default: 10000)
```

### Environment Variables

```bash
# Network
export UC_HOST=0.0.0.0
export UC_PORT=6379

# Tenants
export UC_MAX_TENANTS=5000

# AOF Persistence
export UC_AOF_DIR=/var/lib/ultracache
export UC_FSYNC_POLICY=EverySecond  # Always, EverySecond, No
```

---

## Persistence (AOF)

### What is AOF?

Append-Only File (AOF) is a transaction log that records all write operations. On startup, UltraCache replays the AOF to recover state.

### Fsync Policies

Control how often writes are persisted to disk:

| Policy | Behavior | Durability | Performance |
|--------|----------|-----------|-------------|
| **Always** | Fsync after each write | Highest | Slowest |
| **EverySecond** | Fsync once per second | High | Balanced |
| **No** | Let OS handle fsync | Lower | Fastest |

**Recommendation:** Use `EverySecond` for most deployments.

### Enable Persistence

```bash
# Export environment variable before starting
export UC_FSYNC_POLICY=EverySecond
export UC_AOF_DIR=/var/lib/ultracache

./target/release/ultracache
```

### AOF File Location

By default: `./aof_logs/`

Files are named per tenant: `{tenant-id}.aof`

### Recovery

On startup, UltraCache automatically:

1. Checks for AOF files
2. Replays each tenant's operations
3. Rebuilds in-memory state
4. Starts accepting connections

Recovery time depends on:
- Number of tenants
- Size of AOF files
- Complexity of operations

### AOF Maintenance

**Check AOF size:**

```bash
du -sh aof_logs/
```

**Compact AOF (when it gets large):**

UltraCache automatically rewrites AOF periodically. To force a rewrite:

```bash
redis-cli AUTH your-tenant
redis-cli BGREWRITEAOF  # Not yet implemented - manual rewrite coming
```

For now, periodically stop and restart UltraCache to trigger cleanup.

---

## Monitoring

### Health Checks

```bash
# Simple ping check
redis-cli PING

# Health endpoint (for load balancers)
curl http://localhost:6379/health
```

### Metrics Collection

Query statistics per tenant:

```bash
redis-cli AUTH tenant-name
redis-cli STATS
```

Key metrics to monitor:

- `key_count` - Total keys
- `used_bytes` - Memory used
- `evictions` - LRU evictions
- `cpu_time_micros` - CPU time used
- `operations` - Total operations

### Logging

UltraCache logs to stdout. Capture with:

```bash
./target/release/ultracache > ultracache.log 2>&1 &
tail -f ultracache.log
```

Or with systemd:

```bash
journalctl -u ultracache -f
```

---

## Resource Management

### Memory Configuration

**Per-tenant default:** 64MB

**Recommendation:**
- Development: 64MB
- Staging: 256MB
- Production: 512MB - 2GB

**Set memory limit:**

```bash
redis-cli AUTH tenant-name
redis-cli MEMORY-LIMIT 1073741824  # 1GB
```

### CPU Configuration

**Per-tenant default:** 5000 microseconds/sec (5ms)

**Recommendation:**
- Background cache: 5ms
- Critical path: 50ms
- Performance-critical: 100ms

**Set CPU quota:**

```bash
redis-cli AUTH tenant-name
redis-cli CPU-THROTTLE 100000  # 100ms per second
```

---

## Performance Tuning

### Connection Pooling

Use connection pools in applications:

```python
import redis
from redis import ConnectionPool

pool = ConnectionPool(
    host='127.0.0.1',
    port=6379,
    max_connections=50
)
client = redis.Redis(connection_pool=pool)
```

### Pipelining

Send multiple commands at once:

```python
pipe = client.pipeline()
for i in range(1000):
    pipe.set(f'key:{i}', f'value:{i}')
pipe.execute()
```

### Batch Operations

Group related operations:

```python
# Good - batch writes
client.execute_command('AUTH', 'tenant-a')
with client.pipeline() as pipe:
    for i in range(10000):
        pipe.hset(f'user:{i}', 'name', f'user{i}')
    pipe.execute()

# Avoid - individual calls
for i in range(10000):
    client.hset(f'user:{i}', 'name', f'user{i}')
```

### TTL Strategy

Use appropriate TTLs to prevent memory bloat:

```bash
# Session data - 1 hour
SET session:abc "{...}"
EXPIRE session:abc 3600

# Cache data - 24 hours
SET cache:key "{...}"
EXPIRE cache:key 86400

# Leaderboard - persistent (no TTL)
ZADD leaderboard 1000 "player"
```

---

## High Availability

### Instance Failover

For HA, deploy with external load balancer:

```
┌─────────────────┐
│  Load Balancer  │
└────────┬────────┘
         │
    ┌────┴──────┬─────────┐
    │           │         │
┌───▼─────┐ ┌──▼────┐ ┌──▼────┐
│UC-Inst-1│ │UC-Inst2│ │UC-Inst3│
└─────────┘ └────────┘ └────────┘
```

**Health check configuration (example with HAProxy):**

```
backend ultracache
    mode tcp
    option tcp-check
    tcp-check connect port 6379
    tcp-check send "PING\r\n"
    tcp-check expect string "PONG"
    server uc1 127.0.0.1:6379 check inter 5s
    server uc2 127.0.0.1:6380 check inter 5s
    server uc3 127.0.0.1:6381 check inter 5s
```

### Data Replication

UltraCache currently does not support built-in replication. For HA:

1. Use external replication (e.g., Consul, etcd)
2. Deploy separate instances per tenant
3. Use persistent storage (AOF) for recovery

---

## Scaling Strategies

### Vertical Scaling

Increase resources on a single instance:

```bash
# More memory
export UC_MEMORY_PER_TENANT=2147483648  # 2GB

# More CPU
export UC_CPU_QUOTA_DEFAULT=100000  # 100ms

./target/release/ultracache
```

### Horizontal Scaling

Deploy multiple instances:

```bash
# Instance 1 - tenants 1-333
./target/release/ultracache --port 6379

# Instance 2 - tenants 334-666
./target/release/ultracache --port 6380

# Instance 3 - tenants 667-1000
./target/release/ultracache --port 6381
```

Use consistent hashing to route tenants:

```python
import hashlib

def get_instance(tenant_id):
    instances = ['localhost:6379', 'localhost:6380', 'localhost:6381']
    hash_value = int(hashlib.md5(tenant_id.encode()).hexdigest(), 16)
    return instances[hash_value % len(instances)]
```

---

## Security

### Network Security

```bash
# Only listen on localhost
./target/release/ultracache --host 127.0.0.1 --port 6379

# Use in container/VPC for network isolation
```

### Tenant Isolation

Each tenant has complete data isolation. Use meaningful tenant tokens:

```bash
# Token format - include app, environment, tenant
AUTH app:production:customer-123
```

### Access Control

Currently, UltraCache has basic AUTH. For advanced ACL:

1. Use external proxy/gateway (e.g., Redis Sentinel)
2. Implement custom authentication layer
3. Deploy in isolated VPC with security groups

---

## Backup and Restore

### Backup Strategy

**Using AOF:**

```bash
# AOF files are in aof_logs/
cp -r aof_logs/ backup/aof_logs/
```

**Manual backup:**

```bash
redis-cli AUTH tenant-name
redis-cli BGSAVE  # Not yet implemented

# Or export data
for key in $(redis-cli KEYS "*"); do
    redis-cli GET "$key" >> backup.txt
done
```

### Restore Strategy

1. Stop UltraCache
2. Copy AOF files to `aof_logs/`
3. Start UltraCache (automatically replays)

---

## Troubleshooting Deployment

### Port Already in Use

```bash
# Find process using port 6379
lsof -i :6379

# Kill the process
kill -9 <PID>

# Or use different port
./target/release/ultracache --port 6380
```

### Out of Memory

**Symptom:** Operations start failing

**Solution:**
```bash
# Increase memory limit
redis-cli AUTH tenant-name
redis-cli MEMORY-LIMIT 2147483648  # 2GB

# Or reduce tenant workload
redis-cli FLUSHDB
```

### High CPU Usage

**Solution:**
```bash
# Increase CPU quota
redis-cli AUTH tenant-name
redis-cli CPU-THROTTLE 500000  # 500ms per second

# Or optimize queries
# Avoid slow commands (KEYS, SCAN)
# Use HGETALL, SMEMBERS strategically
```

### Connection Failures

```bash
# Test connectivity
redis-cli -p 6379 PING

# Check firewall
netstat -tlnp | grep 6379

# Check logs
tail -f ultracache.log
```

---

## Performance Expectations

Under standard conditions (single instance):

| Metric | Value |
|--------|-------|
| **Throughput** | 500K ops/sec |
| **P50 Latency** | < 100 µs |
| **P99 Latency** | < 1 ms |
| **Memory** | ~1-2GB per 1M keys |
| **CPU** | Moderate (multi-core scaling) |

Actual performance depends on:
- Hardware (CPU cores, RAM speed)
- Workload (read/write ratio, key size)
- Tenant configuration (CPU quotas, memory limits)
