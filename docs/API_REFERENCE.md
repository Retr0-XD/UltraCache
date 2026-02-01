# API Reference

Complete reference for all UltraCache commands.

## Command Categories

- [Connection](#connection)
- [String Commands](#string-commands)
- [Hash Commands](#hash-commands)
- [Set Commands](#set-commands)
- [Sorted Set Commands](#sorted-set-commands)
- [List Commands](#list-commands)
- [Key Commands](#key-commands)
- [Admin Commands](#admin-commands)

---

## Connection

### PING

Test connection to the server.

**Syntax:**
```
PING
```

**Returns:** "PONG" if connection is successful.

**Example:**
```bash
PING
# Output: PONG
```

---

### AUTH

Authenticate as a specific tenant. All subsequent commands are scoped to this tenant.

**Syntax:**
```
AUTH <tenant-token>
```

**Returns:** "OK" on success.

**Example:**
```bash
AUTH my-tenant-123
# Output: OK
```

---

## String Commands

### SET

Set a string value.

**Syntax:**
```
SET <key> <value>
```

**Returns:** "OK"

**Example:**
```bash
SET username "alice"
# Output: OK
```

---

### GET

Get a string value.

**Syntax:**
```
GET <key>
```

**Returns:** The value, or nil if key doesn't exist.

**Example:**
```bash
GET username
# Output: "alice"

GET nonexistent
# Output: (nil)
```

---

### DEL

Delete one or more keys.

**Syntax:**
```
DEL <key> [<key> ...]
```

**Returns:** Number of keys deleted.

**Example:**
```bash
DEL username email phone
# Output: 3 (if all existed)
```

---

### EXPIRE

Set a key's time-to-live in seconds.

**Syntax:**
```
EXPIRE <key> <seconds>
```

**Returns:** 1 if timeout was set, 0 if key doesn't exist.

**Example:**
```bash
SET session "data"
EXPIRE session 3600
# Output: 1
```

---

### TTL

Get remaining time-to-live in seconds.

**Syntax:**
```
TTL <key>
```

**Returns:** Remaining seconds, -1 if key has no expiration, -2 if key doesn't exist.

**Example:**
```bash
TTL session
# Output: 3599 (approximately, decreases over time)
```

---

## Hash Commands

### HSET

Set hash fields and values.

**Syntax:**
```
HSET <key> <field> <value> [<field> <value> ...]
```

**Returns:** Number of fields added.

**Example:**
```bash
HSET user:1 name "alice" email "alice@example.com"
# Output: 2
```

---

### HGET

Get value of a hash field.

**Syntax:**
```
HGET <key> <field>
```

**Returns:** The value, or nil if field doesn't exist.

**Example:**
```bash
HGET user:1 name
# Output: "alice"
```

---

### HDEL

Delete one or more hash fields.

**Syntax:**
```
HDEL <key> <field> [<field> ...]
```

**Returns:** Number of fields deleted.

**Example:**
```bash
HDEL user:1 email phone
# Output: 2
```

---

### HEXISTS

Check if a hash field exists.

**Syntax:**
```
HEXISTS <key> <field>
```

**Returns:** 1 if field exists, 0 otherwise.

**Example:**
```bash
HEXISTS user:1 name
# Output: 1
```

---

### HLEN

Get number of fields in a hash.

**Syntax:**
```
HLEN <key>
```

**Returns:** Number of fields.

**Example:**
```bash
HLEN user:1
# Output: 3
```

---

### HINCRBY

Increment a hash field by a number.

**Syntax:**
```
HINCRBY <key> <field> <increment>
```

**Returns:** The new value after increment.

**Example:**
```bash
HSET user:1 age 30
HINCRBY user:1 age 1
# Output: 31
```

---

### HGETALL

Get all fields and values of a hash.

**Syntax:**
```
HGETALL <key>
```

**Returns:** Array of alternating fields and values.

**Example:**
```bash
HGETALL user:1
# Output: name, alice, email, alice@example.com, age, 31
```

---

### HKEYS

Get all field names in a hash.

**Syntax:**
```
HKEYS <key>
```

**Returns:** Array of field names.

**Example:**
```bash
HKEYS user:1
# Output: name, email, age
```

---

### HVALS

Get all values in a hash.

**Syntax:**
```
HVALS <key>
```

**Returns:** Array of values.

**Example:**
```bash
HVALS user:1
# Output: alice, alice@example.com, 31
```

---

## Set Commands

### SADD

Add members to a set.

**Syntax:**
```
SADD <key> <member> [<member> ...]
```

**Returns:** Number of members added.

**Example:**
```bash
SADD tags "redis" "cache" "database"
# Output: 3
```

---

### SREM

Remove members from a set.

**Syntax:**
```
SREM <key> <member> [<member> ...]
```

**Returns:** Number of members removed.

**Example:**
```bash
SREM tags "redis"
# Output: 1
```

---

### SMEMBERS

Get all members of a set.

**Syntax:**
```
SMEMBERS <key>
```

**Returns:** Array of all members.

**Example:**
```bash
SMEMBERS tags
# Output: cache, database
```

---

### SCARD

Get cardinality (number of members) of a set.

**Syntax:**
```
SCARD <key>
```

**Returns:** Number of members.

**Example:**
```bash
SCARD tags
# Output: 2
```

---

### SISMEMBER

Check if a member is in a set.

**Syntax:**
```
SISMEMBER <key> <member>
```

**Returns:** 1 if member exists, 0 otherwise.

**Example:**
```bash
SISMEMBER tags "cache"
# Output: 1
```

---

### SINTER

Get intersection of multiple sets.

**Syntax:**
```
SINTER <key> [<key> ...]
```

**Returns:** Array of members in intersection.

**Example:**
```bash
SADD user1:tags "redis" "cache" "python"
SADD user2:tags "cache" "database" "sql"
SINTER user1:tags user2:tags
# Output: cache
```

---

## Sorted Set Commands

### ZADD

Add members with scores to a sorted set.

**Syntax:**
```
ZADD <key> <score> <member> [<score> <member> ...]
```

**Returns:** Number of members added.

**Example:**
```bash
ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
# Output: 3
```

---

### ZREM

Remove members from a sorted set.

**Syntax:**
```
ZREM <key> <member> [<member> ...]
```

**Returns:** Number of members removed.

**Example:**
```bash
ZREM leaderboard "alice"
# Output: 1
```

---

### ZRANGE

Get members in index range (ordered by score, low to high).

**Syntax:**
```
ZRANGE <key> <start> <stop>
```

**Returns:** Array of members in range.

**Example:**
```bash
ZRANGE leaderboard 0 -1
# Output: charlie, bob
```

---

### ZCARD

Get cardinality of a sorted set.

**Syntax:**
```
ZCARD <key>
```

**Returns:** Number of members.

**Example:**
```bash
ZCARD leaderboard
# Output: 2
```

---

### ZSCORE

Get score of a member in a sorted set.

**Syntax:**
```
ZSCORE <key> <member>
```

**Returns:** The score, or nil if member doesn't exist.

**Example:**
```bash
ZSCORE leaderboard "bob"
# Output: 200
```

---

## List Commands

### LPUSH

Push elements to the head of a list.

**Syntax:**
```
LPUSH <key> <element>
```

**Returns:** Length of list after push.

**Example:**
```bash
LPUSH mylist "first"
LPUSH mylist "second"
# Output: 2
```

---

### RPUSH

Push elements to the tail of a list.

**Syntax:**
```
RPUSH <key> <element>
```

**Returns:** Length of list after push.

**Example:**
```bash
RPUSH mylist "third"
# Output: 3
```

---

### LPOP

Remove and return element from head of list.

**Syntax:**
```
LPOP <key>
```

**Returns:** The element, or nil if list is empty.

**Example:**
```bash
LPOP mylist
# Output: "second"
```

---

### RPOP

Remove and return element from tail of list.

**Syntax:**
```
RPOP <key>
```

**Returns:** The element, or nil if list is empty.

**Example:**
```bash
RPOP mylist
# Output: "third"
```

---

### LLEN

Get length of a list.

**Syntax:**
```
LLEN <key>
```

**Returns:** Length of list.

**Example:**
```bash
LLEN mylist
# Output: 1
```

---

### LRANGE

Get range of elements from a list.

**Syntax:**
```
LRANGE <key> <start> <stop>
```

**Returns:** Array of elements in range.

**Example:**
```bash
RPUSH tasks "task1" "task2" "task3"
LRANGE tasks 0 1
# Output: task1, task2
```

---

## Key Commands

### DBSIZE

Get number of keys in current tenant.

**Syntax:**
```
DBSIZE
```

**Returns:** Number of keys.

**Example:**
```bash
DBSIZE
# Output: 42
```

---

### FLUSHDB

Delete all keys in current tenant.

**Syntax:**
```
FLUSHDB
```

**Returns:** "OK"

**Example:**
```bash
FLUSHDB
# Output: OK
```

---

## Admin Commands

### TENANTS

List all registered tenants.

**Syntax:**
```
TENANTS
```

**Returns:** Array of tenant information.

**Example:**
```bash
TENANTS
# Output:
# id=default memory_limit_bytes=67108864 cpu_quota_micros=5000
# id=tenant-a memory_limit_bytes=67108864 cpu_quota_micros=5000
# id=tenant-b memory_limit_bytes=67108864 cpu_quota_micros=5000
```

---

### STATS

Get server statistics.

**Syntax:**
```
STATS
```

**Returns:** Array of statistics.

**Example:**
```bash
STATS
# Output: Multiple stat lines with key=value pairs
```

---

### CPU-THROTTLE

Set CPU quota for current tenant (microseconds per second).

**Syntax:**
```
CPU-THROTTLE <microseconds>
```

**Returns:** "OK"

**Example:**
```bash
CPU-THROTTLE 10000
# Output: OK
```

---

### MEMORY-LIMIT

Set memory limit for current tenant (bytes).

**Syntax:**
```
MEMORY-LIMIT <bytes>
```

**Returns:** "OK"

**Example:**
```bash
MEMORY-LIMIT 104857600
# Output: OK (100MB)
```

---

### QUIT

Close the connection.

**Syntax:**
```
QUIT
```

**Returns:** "OK"

**Example:**
```bash
QUIT
# Output: OK (connection closes)
```

---

## Error Codes

Common error responses:

| Error | Meaning |
|-------|---------|
| `ERR unknown command` | Command is not implemented |
| `ERR wrong number of arguments` | Invalid number of arguments |
| `WRONGTYPE Operation against a key holding the wrong kind of value` | Type mismatch (e.g., using LPUSH on a string) |
| `ERR invalid token` | Invalid tenant token in AUTH |
| `ERR memory limit exceeded` | Tenant exceeded memory quota |
| `ERR cpu quota exceeded` | Tenant exceeded CPU quota |

---

## Type System

UltraCache enforces strict typing. Operations on wrong types return WRONGTYPE error:

```bash
SET mykey "string"
LPUSH mykey "element"
# Error: WRONGTYPE Operation against a key holding the wrong kind of value
```

To use a different type, delete the key first:

```bash
DEL mykey
LPUSH mykey "element"
# Output: 1
```
