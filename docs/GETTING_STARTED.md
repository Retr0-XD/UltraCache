# Getting Started with UltraCache

This guide covers installation, building, and basic setup for UltraCache.

## Prerequisites

- **Rust 1.83+** (2021 edition)
- **Linux/macOS** or WSL on Windows
- **cargo** (comes with Rust)
- Optional: **Docker** for containerized deployment

## Installation

### From Source

1. Clone the repository:
   ```bash
   git clone https://github.com/Retr0-XD/UltraCache.git
   cd UltraCache
   ```

2. Build the release binary:
   ```bash
   cargo build --release
   ```
   The binary will be at `target/release/ultracache`

3. Run the server:
   ```bash
   ./target/release/ultracache
   ```
   Server starts on `127.0.0.1:6379` by default

### Using Docker

1. Build the Docker image:
   ```bash
   docker build -t ultracache:latest .
   ```

2. Run a container:
   ```bash
   docker run -p 6379:6379 ultracache:latest
   ```

3. (Optional) Run with custom configuration:
   ```bash
   docker run -p 6379:6379 \
     -e UC_HOST=0.0.0.0 \
     -e UC_PORT=6379 \
     ultracache:latest
   ```

## Connecting to UltraCache

### Using redis-cli

If redis-tools is installed:

```bash
redis-cli -p 6379

# You should see the prompt:
127.0.0.1:6379>

# Try a simple command:
PING
# Response: PONG
```

### Using Python

```python
import socket

def send_command(cmd):
    sock = socket.create_connection(("127.0.0.1", 6379), timeout=2)
    sock.sendall(cmd.encode() + b"\r\n")
    response = sock.recv(1024)
    sock.close()
    return response.decode()

# Send PING
print(send_command("PING"))  # Response: +PONG
```

### Using Any Redis-Compatible Client

UltraCache implements the Redis Serialization Protocol (RESP), so any Redis client works:

- **Python**: redis-py
- **Node.js**: redis, ioredis
- **Java**: Jedis, Lettuce
- **Go**: go-redis
- **Ruby**: redis-rb

Example with Python redis-py:

```python
import redis

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
r.set('key', 'value')
print(r.get('key'))  # Output: 'value'
```

---

## Basic Operations

### String Operations

```bash
# Set a string value
SET mykey "Hello"

# Get a string value
GET mykey
# Output: "Hello"

# Delete a key
DEL mykey

# Set with expiration (in seconds)
SET session "data" EX 3600

# Check time-to-live
TTL session
# Output: (remaining seconds)
```

### Hash Operations

```bash
# Set hash fields
HSET user:1 name "Alice" email "alice@example.com" age 30

# Get a single field
HGET user:1 name
# Output: "Alice"

# Get all fields
HGETALL user:1
# Output: name, Alice, email, alice@example.com, age, 30

# Get all keys
HKEYS user:1
# Output: name, email, age

# Get all values
HVALS user:1
# Output: Alice, alice@example.com, 30

# Increment a field
HINCRBY user:1 age 1
# Output: 31

# Delete a field
HDEL user:1 age
```

### Set Operations

```bash
# Add members to a set
SADD tags "redis" "cache" "database"

# Get all members
SMEMBERS tags
# Output: redis, cache, database

# Check membership
SISMEMBER tags "redis"
# Output: 1 (true)

# Get set cardinality
SCARD tags
# Output: 3

# Set intersection
SADD user1:tags "redis" "cache" "python"
SADD user2:tags "cache" "database" "sql"
SINTER user1:tags user2:tags
# Output: cache

# Remove a member
SREM tags "redis"
```

### Sorted Set Operations

```bash
# Add members with scores
ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"

# Get all members in range
ZRANGE leaderboard 0 -1
# Output: alice, charlie, bob

# Get score of a member
ZSCORE leaderboard "bob"
# Output: 200

# Get cardinality
ZCARD leaderboard
# Output: 3

# Remove a member
ZREM leaderboard "alice"
```

### List Operations

```bash
# Push to left
LPUSH mylist "first" "second"

# Push to right
RPUSH mylist "third"

# Get range
LRANGE mylist 0 -1
# Output: second, first, third

# Get length
LLEN mylist
# Output: 3

# Pop from left
LPOP mylist
# Output: second

# Pop from right
RPOP mylist
# Output: third
```

---

## Multi-Tenancy

### Authenticating as a Tenant

All operations are scoped to the current tenant. Default tenant is "default".

```bash
# Authenticate as a specific tenant
AUTH my-tenant-token

# All subsequent operations are scoped to this tenant
SET key "value"
GET key
# Output: "value"

# Switch to a different tenant
AUTH other-tenant-token

# This tenant has separate data
GET key
# Output: (nil) - key doesn't exist in this tenant

# List all registered tenants
TENANTS
```

### Tenant Isolation Example

```python
import redis

client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

# Tenant A
client.execute_command('AUTH', 'tenant-a')
client.set('data', 'from-a')

# Tenant B
client.execute_command('AUTH', 'tenant-b')
client.set('data', 'from-b')

# Back to Tenant A
client.execute_command('AUTH', 'tenant-a')
print(client.get('data'))  # Output: from-a

# Tenant B still has its own data
client.execute_command('AUTH', 'tenant-b')
print(client.get('data'))  # Output: from-b
```

---

## Running Tests

### Unit Tests

```bash
# Run Rust unit tests
cargo test --release
```

### Integration Tests

```bash
# Run all integration tests
for test in tests/test_*.py; do
  python3 "$test"
done

# Or individual tests
python3 tests/test_list.py
python3 tests/test_hash_extended.py
python3 tests/test_set.py
```

---

## Troubleshooting

### Server won't start

**Issue:** "Address already in use" error

**Solution:** 
- Port 6379 is already in use. Either:
  - Stop the existing process: `pkill -9 ultracache`
  - Run on a different port: `./target/release/ultracache --port 6380`

### Can't connect from redis-cli

**Issue:** Connection refused

**Solution:**
- Ensure UltraCache is running
- Check the port matches: `redis-cli -p 6379`
- Verify with: `netstat -tlnp | grep 6379`

### Commands return "unknown command" error

**Issue:** Command not implemented

**Solution:**
- UltraCache supports a subset of Redis commands
- See [API_REFERENCE.md](API_REFERENCE.md) for supported commands
- Not all Redis commands are implemented (e.g., KEYS without pagination, SCAN variants)

### Performance issues

**Issue:** Slow responses or high latency

**Solutions:**
- Check CPU usage: `top`
- Check memory usage: `free -m`
- Ensure you're using the release binary: `./target/release/ultracache`
- See [DEPLOYMENT.md](DEPLOYMENT.md) for tuning options

---

## Next Steps

- Read [API_REFERENCE.md](API_REFERENCE.md) for complete command reference
- Review [MULTI_TENANCY.md](MULTI_TENANCY.md) for advanced tenant management
- See [DEPLOYMENT.md](DEPLOYMENT.md) for production setup
- Check [ARCHITECTURE.md](ARCHITECTURE.md) for internal design details
