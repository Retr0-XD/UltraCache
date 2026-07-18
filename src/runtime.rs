use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use tokio::sync::{mpsc, oneshot};

use crate::persistence::AofManager;
use crate::resp::RespValue;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Stats, // Admin command for tenant stats
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Vec<u8>,
        /// Optional TTL in seconds (EX) or milliseconds (PX).
        ex: Option<i64>,
        px: Option<i64>,
        /// NX: only set if key does not exist. XX: only set if key exists.
        nx: bool,
        xx: bool,
    },
    GetSet {
        key: String,
        value: Vec<u8>,
    },
    StrLen {
        key: String,
    },
    Del {
        key: String,
    },
    Expire {
        key: String,
        seconds: i64,
    },
    Ttl {
        key: String,
    },
    // Hash commands
    Hget {
        key: String,
        field: String,
    },
    Hset {
        key: String,
        field: String,
        value: Vec<u8>,
    },
    Hdel {
        key: String,
        field: String,
    },
    Hincrby {
        key: String,
        field: String,
        delta: i64,
    },
    Hincrbyfloat {
        key: String,
        field: String,
        delta: f64,
    },
    Hsetnx {
        key: String,
        field: String,
        value: Vec<u8>,
    },
    Hgetall {
        key: String,
    },
    Hkeys {
        key: String,
    },
    Hvals {
        key: String,
    },
    // Set commands
    Sadd {
        key: String,
        member: String,
    },
    Srem {
        key: String,
        member: String,
    },
    Smove {
        source: String,
        destination: String,
        member: String,
    },
    Spop {
        key: String,
    },
    Smembers {
        key: String,
    },
    Scard {
        key: String,
    },
    Sismember {
        key: String,
        member: String,
    },
    // Sorted Set commands
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
    Zincrby {
        key: String,
        delta: f64,
        member: String,
    },
    Zrem {
        key: String,
        member: String,
    },
    Zrangebyscore {
        key: String,
        min: String,
        max: String,
    },
    Zremrangebyscore {
        key: String,
        min: String,
        max: String,
    },
    Zcount {
        key: String,
        min: String,
        max: String,
    },
    Zrange {
        key: String,
        start: i64,
        stop: i64,
    },
    Zcard {
        key: String,
    },
    Zscore {
        key: String,
        member: String,
    },
    // List commands
    Lpush {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Lpushx {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Rpush {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Rpushx {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Lindex {
        key: String,
        index: i64,
    },
    Lset {
        key: String,
        index: i64,
        value: Vec<u8>,
    },
    Ltrim {
        key: String,
        start: i64,
        stop: i64,
    },
    Linsert {
        key: String,
        before: bool,
        pivot: Vec<u8>,
        value: Vec<u8>,
    },
    Lpop {
        key: String,
    },
    Rpop {
        key: String,
    },
    Llen {
        key: String,
    },
    Lrange {
        key: String,
        start: i64,
        stop: i64,
    },
    // String extensions
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    Incrby {
        key: String,
        delta: i64,
    },
    Decrby {
        key: String,
        delta: i64,
    },
    Append {
        key: String,
        value: Vec<u8>,
    },
    Exists {
        key: String,
    },
    Type {
        key: String,
    },
    Persist {
        key: String,
    },
    Pttl {
        key: String,
    },
    Pexpire {
        key: String,
        milliseconds: i64,
    },
    Pexpireat {
        key: String,
        milliseconds_timestamp: i64,
    },
    Expireat {
        key: String,
        timestamp: i64,
    },
    // Bulk / multi-key commands
    Mset {
        pairs: Vec<(String, Vec<u8>)>,
    },
    Mget {
        keys: Vec<String>,
    },
    Keys {
        pattern: String,
    },
    Flushdb,
    Rename {
        from: String,
        to: String,
    },
    Dbsize,
    Randomkey,
    Echo {
        message: String,
    },
    Info,
    ConfigGet {
        parameter: String,
    },
    ConfigSet {
        parameter: String,
        value: String,
    },
    // Sorted set extensions
    Zrank {
        key: String,
        member: String,
    },
    Zrevrange {
        key: String,
        start: i64,
        stop: i64,
    },
    // Set extensions
    Sunion {
        keys: Vec<String>,
    },
    Sdiff {
        keys: Vec<String>,
    },
}

#[derive(Debug)]
pub struct ShardRequest {
    pub tenant_id: String,
    pub tenant_limit_bytes: u64,
    pub cpu_quota_micros: u64,
    pub command: Command,
    pub respond_to: oneshot::Sender<RespValue>,
}

pub struct ShardRuntime {
    shards: Vec<mpsc::Sender<ShardRequest>>,
    /// Optional append-only file persistence. When present, mutating
    /// commands are logged per tenant for crash recovery.
    persistence: Option<Arc<AofManager>>,
}

impl ShardRuntime {
    pub fn new(num_shards: usize) -> Self {
        Self::with_persistence(num_shards, None)
    }

    pub fn with_persistence(num_shards: usize, persistence: Option<Arc<AofManager>>) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (tx, mut rx) = mpsc::channel::<ShardRequest>(1024);
            shards.push(tx);

            let persistence = persistence.clone();
            tokio::spawn(async move {
                let mut store: HashMap<String, TenantState> = HashMap::new();
                while let Some(req) = rx.recv().await {
                    let response = handle_command(
                        &mut store,
                        req.tenant_id,
                        req.tenant_limit_bytes,
                        req.cpu_quota_micros,
                        req.command,
                        persistence.as_deref(),
                    );
                    let _ = req.respond_to.send(response);
                }
            });
        }

        Self {
            shards,
            persistence,
        }
    }

    /// Replay persisted AOF commands for every known tenant into the shards.
    /// Must be called once at startup before serving traffic.
    pub async fn recover(&self) -> Result<(), String> {
        let Some(aof) = &self.persistence else {
            return Ok(());
        };

        // Discover tenant AOF files on disk.
        let tenants = aof.list_tenants();
        for tenant_id in tenants {
            let commands = aof
                .replay_commands(&tenant_id)
                .map_err(|e| format!("aof replay failed: {e}"))?;
            for cmd in commands {
                // Replay only mutating commands; skip read-only ones.
                let is_mutating = matches!(
                    cmd.first().map(|s| s.to_uppercase()).as_deref(),
                    Some("SET")
                        | Some("DEL")
                        | Some("EXPIRE")
                        | Some("HSET")
                        | Some("HDEL")
                        | Some("HINCRBY")
                        | Some("SADD")
                        | Some("SREM")
                        | Some("ZADD")
                        | Some("ZREM")
                        | Some("LPUSH")
                        | Some("RPUSH")
                        | Some("LPOP")
                        | Some("RPOP")
                        | Some("INCR")
                        | Some("DECR")
                        | Some("INCRBY")
                        | Some("DECRBY")
                        | Some("APPEND")
                        | Some("PERSIST")
                        | Some("MSET")
                        | Some("FLUSHDB")
                        | Some("RENAME")
                );
                if !is_mutating {
                    continue;
                }
                let command = parse_args_to_command(&cmd)
                    .ok_or_else(|| "failed to parse aof command".to_string())?;
                self.execute(tenant_id.clone(), 64 * 1024 * 1024, 5_000, command)
                    .await;
            }
        }
        Ok(())
    }

    pub async fn stats(
        &self,
        tenant_id: String,
        tenant_limit_bytes: u64,
        cpu_quota_micros: u64,
    ) -> RespValue {
        let mut receivers = Vec::with_capacity(self.shards.len());

        for tx in &self.shards {
            let (resp_tx, resp_rx) = oneshot::channel();
            let req = ShardRequest {
                tenant_id: tenant_id.clone(),
                tenant_limit_bytes,
                cpu_quota_micros,
                command: Command::Stats,
                respond_to: resp_tx,
            };

            if tx.send(req).await.is_err() {
                return RespValue::Error("ERR shard unavailable".to_string());
            }
            receivers.push(resp_rx);
        }

        let mut agg = StatsAggregate::new(self.shards.len() as u64);

        for rx in receivers {
            match rx.await {
                Ok(RespValue::BulkString(Some(bytes))) => {
                    let stats_str = String::from_utf8_lossy(&bytes);
                    let parsed = parse_stats_kv(&stats_str);
                    agg.absorb(&parsed);
                }
                Ok(RespValue::Error(msg)) => return RespValue::Error(msg),
                Ok(_) => return RespValue::Error("ERR invalid stats response".to_string()),
                Err(_) => return RespValue::Error("ERR shard response failed".to_string()),
            }
        }

        RespValue::BulkString(Some(agg.format(&tenant_id).into_bytes()))
    }

    pub async fn execute(
        &self,
        tenant_id: String,
        tenant_limit_bytes: u64,
        cpu_quota_micros: u64,
        command: Command,
    ) -> RespValue {
        // SMOVE spans two keys (source and destination) that may live on
        // different shards. A single-shard handler cannot move a member across
        // shards, so we coordinate it here: remove from the source shard, then
        // (if removed) add to the destination shard. The member value is known
        // up front, so no cross-shard data transfer is required.
        if let Command::Smove {
            source,
            destination,
            member,
        } = &command
        {
            let src_idx = self.route_shard(
                &tenant_id,
                &Command::Srem {
                    key: source.clone(),
                    member: member.clone(),
                },
            );
            let (tx, rx) = oneshot::channel();
            if self.shards[src_idx]
                .send(ShardRequest {
                    tenant_id: tenant_id.clone(),
                    tenant_limit_bytes,
                    cpu_quota_micros,
                    command: Command::Srem {
                        key: source.clone(),
                        member: member.clone(),
                    },
                    respond_to: tx,
                })
                .await
                .is_err()
            {
                return RespValue::Error("ERR shard unavailable".to_string());
            }
            let removed = match rx.await {
                Ok(RespValue::Integer(n)) => n,
                Ok(RespValue::Error(_)) => 0,
                _ => 0,
            };
            if removed != 1 {
                return RespValue::Integer(0);
            }
            let dst_idx = self.route_shard(
                &tenant_id,
                &Command::Sadd {
                    key: destination.clone(),
                    member: member.clone(),
                },
            );
            let (tx, rx) = oneshot::channel();
            if self.shards[dst_idx]
                .send(ShardRequest {
                    tenant_id,
                    tenant_limit_bytes,
                    cpu_quota_micros,
                    command: Command::Sadd {
                        key: destination.clone(),
                        member: member.clone(),
                    },
                    respond_to: tx,
                })
                .await
                .is_err()
            {
                return RespValue::Error("ERR shard unavailable".to_string());
            }
            return match rx.await {
                Ok(v) => v,
                Err(_) => RespValue::Error("ERR shard response failed".to_string()),
            };
        }

        // FLUSHDB / DBSIZE / RANDOMKEY are global (key-less) commands that must
        // operate across every shard, not just the one implied by the tenant-id
        // hash. Route them by broadcasting to all shards and aggregating.
        if matches!(
            &command,
            Command::Flushdb | Command::Dbsize | Command::Randomkey
        ) {
            let mut receivers = Vec::with_capacity(self.shards.len());
            for tx in &self.shards {
                let (resp_tx, resp_rx) = oneshot::channel();
                let req = ShardRequest {
                    tenant_id: tenant_id.clone(),
                    tenant_limit_bytes,
                    cpu_quota_micros,
                    command: command.clone(),
                    respond_to: resp_tx,
                };
                if tx.send(req).await.is_err() {
                    return RespValue::Error("ERR shard unavailable".to_string());
                }
                receivers.push(resp_rx);
            }
            match &command {
                Command::Flushdb => {
                    for rx in receivers {
                        if let Ok(RespValue::Error(msg)) = rx.await {
                            return RespValue::Error(msg);
                        }
                    }
                    return RespValue::SimpleString("OK".to_string());
                }
                Command::Dbsize => {
                    let mut total: i64 = 0;
                    for rx in receivers {
                        match rx.await {
                            Ok(RespValue::Integer(n)) => total += n,
                            Ok(RespValue::Error(msg)) => return RespValue::Error(msg),
                            _ => return RespValue::Error("ERR invalid response".to_string()),
                        }
                    }
                    return RespValue::Integer(total);
                }
                Command::Randomkey => {
                    for rx in receivers {
                        match rx.await {
                            Ok(RespValue::BulkString(Some(bytes))) => {
                                return RespValue::BulkString(Some(bytes));
                            }
                            Ok(RespValue::BulkString(None)) => continue,
                            Ok(RespValue::Error(msg)) => return RespValue::Error(msg),
                            _ => return RespValue::Error("ERR invalid response".to_string()),
                        }
                    }
                    return RespValue::BulkString(None);
                }
                _ => unreachable!(),
            }
        }

        let shard_idx = self.route_shard(&tenant_id, &command);
        let (tx, rx) = oneshot::channel();
        let req = ShardRequest {
            tenant_id,
            tenant_limit_bytes,
            cpu_quota_micros,
            command,
            respond_to: tx,
        };

        if self.shards[shard_idx].send(req).await.is_err() {
            return RespValue::Error("ERR shard unavailable".to_string());
        }

        match rx.await {
            Ok(v) => v,
            Err(_) => RespValue::Error("ERR shard response failed".to_string()),
        }
    }

    pub async fn sinter(
        &self,
        tenant_id: String,
        tenant_limit_bytes: u64,
        cpu_quota_micros: u64,
        keys: Vec<String>,
    ) -> RespValue {
        if keys.is_empty() {
            return RespValue::Array(vec![]);
        }

        let mut intersection: Option<HashSet<String>> = None;

        for key in keys {
            let resp = self
                .execute(
                    tenant_id.clone(),
                    tenant_limit_bytes,
                    cpu_quota_micros,
                    Command::Smembers { key },
                )
                .await;

            match resp {
                RespValue::Array(values) => {
                    let set: HashSet<String> = values
                        .into_iter()
                        .filter_map(|value| match value {
                            RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes).ok(),
                            _ => None,
                        })
                        .collect();

                    intersection = match intersection {
                        None => Some(set),
                        Some(current) => Some(
                            current
                                .intersection(&set)
                                .cloned()
                                .collect::<HashSet<String>>(),
                        ),
                    };

                    if intersection.as_ref().is_none_or(|s| s.is_empty()) {
                        return RespValue::Array(vec![]);
                    }
                }
                RespValue::Error(msg) => return RespValue::Error(msg),
                _ => return RespValue::Error("ERR invalid response".to_string()),
            }
        }

        let members: Vec<RespValue> = intersection
            .unwrap_or_default()
            .into_iter()
            .map(|m| RespValue::BulkString(Some(m.into_bytes())))
            .collect();
        RespValue::Array(members)
    }

    fn route_shard(&self, tenant_id: &str, command: &Command) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tenant_id.hash(&mut hasher);
        match command {
            Command::Get { key }
            | Command::Set { key, .. }
            | Command::GetSet { key, .. }
            | Command::StrLen { key }
            | Command::Del { key }
            | Command::Expire { key, .. }
            | Command::Pexpire { key, .. }
            | Command::Pexpireat { key, .. }
            | Command::Expireat { key, .. }
            | Command::Ttl { key }
            | Command::Hget { key, .. }
            | Command::Hset { key, .. }
            | Command::Hdel { key, .. }
            | Command::Hincrby { key, .. }
            | Command::Hincrbyfloat { key, .. }
            | Command::Hsetnx { key, .. }
            | Command::Hgetall { key }
            | Command::Hkeys { key }
            | Command::Hvals { key }
            | Command::Sadd { key, .. }
            | Command::Srem { key, .. }
            | Command::Smembers { key }
            | Command::Scard { key }
            | Command::Sismember { key, .. }
            | Command::Spop { key }
            | Command::Zadd { key, .. }
            | Command::Zrem { key, .. }
            | Command::Zincrby { key, .. }
            | Command::Zrange { key, .. }
            | Command::Zrangebyscore { key, .. }
            | Command::Zremrangebyscore { key, .. }
            | Command::Zcount { key, .. }
            | Command::Zcard { key }
            | Command::Zscore { key, .. }
            | Command::Lpush { key, .. }
            | Command::Lpushx { key, .. }
            | Command::Rpush { key, .. }
            | Command::Rpushx { key, .. }
            | Command::Lpop { key }
            | Command::Rpop { key }
            | Command::Lindex { key, .. }
            | Command::Lset { key, .. }
            | Command::Ltrim { key, .. }
            | Command::Linsert { key, .. }
            | Command::Llen { key }
            | Command::Lrange { key, .. }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::Incrby { key, .. }
            | Command::Decrby { key, .. }
            | Command::Append { key, .. }
            | Command::Exists { key }
            | Command::Type { key }
            | Command::Persist { key }
            | Command::Pttl { key }
            | Command::Rename { from: key, .. } => key.hash(&mut hasher),
            Command::Mset { pairs } => {
                for (k, _) in pairs {
                    k.hash(&mut hasher);
                }
            }
            Command::Mget { keys } | Command::Sunion { keys } | Command::Sdiff { keys } => {
                for k in keys {
                    k.hash(&mut hasher);
                }
            }
            Command::Keys { pattern: key } => {
                key.hash(&mut hasher);
            }
            Command::Zrank { key, .. } | Command::Zrevrange { key, .. } => {
                key.hash(&mut hasher);
            }
            Command::Smove { source, .. } => {
                // Route by the source key only so the source set is found on the
                // same shard where it was created (e.g. by SADD). Hashing the
                // destination too would send SMOVE to a different shard and the
                // source key would appear missing.
                source.hash(&mut hasher);
            }
            Command::Flushdb
            | Command::Ping
            | Command::Stats
            | Command::Dbsize
            | Command::Randomkey
            | Command::Echo { .. }
            | Command::Info
            | Command::ConfigGet { .. }
            | Command::ConfigSet { .. } => {}
        }
        let hash = hasher.finish() as usize;
        hash % self.shards.len()
    }
}

/// Parse a ZSET min/max bound. Supports `-inf`, `+inf`, and an optional `(`
/// exclusive prefix (Redis convention). Returns `(value, is_exclusive)`.
fn parse_zbound(s: &str) -> (f64, bool) {
    match s {
        "-inf" => (f64::NEG_INFINITY, false),
        "+inf" => (f64::INFINITY, false),
        _ => {
            if let Some(rest) = s.strip_prefix('(') {
                (rest.parse::<f64>().unwrap_or(f64::NEG_INFINITY), true)
            } else {
                (s.parse::<f64>().unwrap_or(f64::INFINITY), false)
            }
        }
    }
}

fn handle_command(
    store: &mut HashMap<String, TenantState>,
    tenant_id: String,
    tenant_limit_bytes: u64,
    cpu_quota_micros: u64,
    command: Command,
    persistence: Option<&AofManager>,
) -> RespValue {
    let state = store
        .entry(tenant_id.clone())
        .or_insert_with(|| TenantState::new(tenant_limit_bytes, cpu_quota_micros));

    if state.limit_bytes != tenant_limit_bytes {
        state.limit_bytes = tenant_limit_bytes;
    }
    if state.cpu_quota_micros != cpu_quota_micros {
        state.cpu_quota_micros = cpu_quota_micros;
    }

    // Reset CPU budget if 1 second has elapsed
    state.check_and_reset_cpu_budget();

    // Check if tenant is throttled
    if state.is_throttled() {
        return RespValue::Error("ERR tenant CPU quota exceeded".to_string());
    }

    let start = std::time::Instant::now();
    let result = match command {
        Command::Ping => RespValue::SimpleString("PONG".to_string()),
        Command::Stats => {
            // Return tenant stats as a bulk string (formatted text)
            let p99 = state.calculate_p99();
            let key_count = state.cache.len();
            let stats = format!(
                "tenant_id: {}\n\
                 memory_used_bytes: {}\n\
                 memory_limit_bytes: {}\n\
                 memory_usage_pct: {:.2}\n\
                 cpu_used_micros: {}\n\
                 cpu_quota_micros: {}\n\
                 cpu_usage_pct: {:.2}\n\
                 total_commands: {}\n\
                 eviction_count: {}\n\
                 key_count: {}\n\
                 latency_p99_micros: {}\n\
                 latency_p99_ms: {:.3}",
                tenant_id,
                state.used_bytes,
                state.limit_bytes,
                (state.used_bytes as f64 / state.limit_bytes as f64) * 100.0,
                state.cpu_used_micros,
                state.cpu_quota_micros,
                (state.cpu_used_micros as f64 / state.cpu_quota_micros as f64) * 100.0,
                state.total_commands,
                state.eviction_count,
                key_count,
                p99,
                p99 as f64 / 1000.0
            );
            RespValue::BulkString(Some(stats.into_bytes()))
        }
        Command::Get { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::BulkString(None)
                    } else {
                        match &entry.data {
                            EntryData::String(bytes) => RespValue::BulkString(Some(bytes.clone())),
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Set {
            ref key,
            ref value,
            ex,
            px,
            nx,
            xx,
        } => {
            let k = tenant_key(&tenant_id, key);
            let exists = state.cache.peek(&k).is_some();

            // Conditional semantics: NX requires the key to be absent, XX
            // requires it to be present.
            if nx && exists {
                return RespValue::BulkString(None);
            }
            if xx && !exists {
                return RespValue::BulkString(None);
            }

            let entry_size = entry_size_bytes(&k, value);
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }

            if let Some(existing) = state.cache.pop(&k) {
                state.used_bytes = state.used_bytes.saturating_sub(existing.size);
            }

            let mut projected = state.used_bytes + entry_size;
            while projected > state.limit_bytes {
                if let Some((_ek, ev)) = state.cache.pop_lru() {
                    state.used_bytes = state.used_bytes.saturating_sub(ev.size);
                    state.record_eviction();
                    projected = state.used_bytes + entry_size;
                } else {
                    return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                }
            }

            // Resolve TTL: PX (millis) takes precedence over EX (seconds).
            let expires_at = if let Some(ms) = px {
                if ms <= 0 {
                    None
                } else {
                    Some(current_timestamp() + (ms as u64).div_ceil(1000))
                }
            } else if let Some(secs) = ex {
                if secs <= 0 {
                    None
                } else {
                    Some(current_timestamp() + secs as u64)
                }
            } else {
                None
            };

            let entry = Entry {
                data: EntryData::String(value.to_vec()),
                expires_at,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::SimpleString("OK".to_string())
        }
        Command::GetSet { ref key, ref value } => {
            let k = tenant_key(&tenant_id, key);
            let entry_size = entry_size_bytes(&k, value);
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }

            // Capture the previous string value (for the response) before
            // replacing. Non-string keys are overwritten and return nil.
            let previous: Option<Vec<u8>> = state.cache.get(&k).and_then(|e| {
                if TenantState::is_expired(e) {
                    None
                } else if let EntryData::String(bytes) = &e.data {
                    Some(bytes.clone())
                } else {
                    None
                }
            });

            if let Some(existing) = state.cache.pop(&k) {
                state.used_bytes = state.used_bytes.saturating_sub(existing.size);
            }

            let mut projected = state.used_bytes + entry_size;
            while projected > state.limit_bytes {
                if let Some((_ek, ev)) = state.cache.pop_lru() {
                    state.used_bytes = state.used_bytes.saturating_sub(ev.size);
                    state.record_eviction();
                    projected = state.used_bytes + entry_size;
                } else {
                    return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                }
            }

            let entry = Entry {
                data: EntryData::String(value.to_vec()),
                expires_at: None,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);

            match previous {
                Some(bytes) => RespValue::BulkString(Some(bytes)),
                None => RespValue::BulkString(None),
            }
        }
        Command::StrLen { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Integer(0);
                    }
                    match &entry.data {
                        EntryData::String(bytes) => RespValue::Integer(bytes.len() as i64),
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Del { ref key } => {
            let k = tenant_key(&tenant_id, key);
            let removed = state.remove(&k);
            if removed.is_some() {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Expire { ref key, seconds } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if seconds <= 0 {
                    state.remove(&k);
                    return RespValue::Integer(1);
                }
                entry.expires_at = Some(current_timestamp() + seconds as u64);
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Ttl { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(-2);
                }
                if let Some(expires_at) = entry.expires_at {
                    let now = current_timestamp();
                    let ttl = (expires_at as i64) - (now as i64);
                    RespValue::Integer(ttl.max(0))
                } else {
                    RespValue::Integer(-1)
                }
            } else {
                RespValue::Integer(-2)
            }
        }
        // Hash commands
        Command::Hget { ref key, ref field } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::BulkString(None);
                    }
                    match &entry.data {
                        EntryData::Hash(map) => match map.get(field) {
                            Some(val) => RespValue::BulkString(Some(val.clone())),
                            None => RespValue::BulkString(None),
                        },
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Hset {
            ref key,
            ref field,
            ref value,
        } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            let is_new_field = map.insert(field.clone(), value.clone()).is_none();
                            reconcile_entry_size(state, &k);
                            return RespValue::Integer(if is_new_field { 1 } else { 0 });
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            // Create new hash
            let mut map = HashMap::new();
            map.insert(field.clone(), value.clone());
            let size = calculate_entry_size(&k, &EntryData::Hash(map.clone()));
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Hdel { ref key, ref field } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::Hash(map) => {
                        let removed = map.remove(field).is_some();
                        reconcile_entry_size(state, &k);
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Hincrby {
            ref key,
            ref field,
            delta,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            let current = match map.get(field) {
                                Some(val) => match std::str::from_utf8(val)
                                    .ok()
                                    .and_then(|s| s.parse::<i64>().ok())
                                {
                                    Some(num) => num,
                                    None => {
                                        return RespValue::Error(
                                            "ERR hash value is not an integer".to_string(),
                                        );
                                    }
                                },
                                None => 0,
                            };
                            let next = current.saturating_add(delta);
                            map.insert(field.to_string(), next.to_string().into_bytes());
                            reconcile_entry_size(state, &k);
                            return RespValue::Integer(next);
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            let mut map = HashMap::new();
            let next = delta;
            map.insert(field.clone(), next.to_string().into_bytes());
            let size = calculate_entry_size(&k, &EntryData::Hash(map.clone()));
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::Integer(next)
        }
        Command::Hincrbyfloat {
            ref key,
            ref field,
            delta,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            let current = match map.get(field) {
                                Some(val) => match std::str::from_utf8(val)
                                    .ok()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    Some(num) => num,
                                    None => {
                                        return RespValue::Error(
                                            "ERR hash value is not a float".to_string(),
                                        );
                                    }
                                },
                                None => 0.0,
                            };
                            let next = current + delta;
                            // Normalize to avoid "-0" and excessive precision.
                            let encoded = format!("{next:.17}").into_bytes();
                            map.insert(field.to_string(), encoded);
                            reconcile_entry_size(state, &k);
                            return RespValue::BulkString(Some(next.to_string().into_bytes()));
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            let mut map = HashMap::new();
            let next = delta;
            map.insert(field.clone(), next.to_string().into_bytes());
            let size = calculate_entry_size(&k, &EntryData::Hash(map.clone()));
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::BulkString(Some(next.to_string().into_bytes()))
        }
        Command::Hsetnx {
            ref key,
            ref field,
            ref value,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            if map.contains_key(field) {
                                return RespValue::Integer(0);
                            }
                            let size = calculate_entry_size(&k, &EntryData::Hash(map.clone()))
                                + (field.len() + value.len()) as u64;
                            if size > state.limit_bytes {
                                return RespValue::Error(
                                    "ERR tenant memory limit exceeded".to_string(),
                                );
                            }
                            map.insert(field.to_string(), value.to_vec());
                            reconcile_entry_size(state, &k);
                            return RespValue::Integer(1);
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            let mut map = HashMap::new();
            map.insert(field.clone(), value.to_vec());
            let size = calculate_entry_size(&k, &EntryData::Hash(map.clone()));
            if size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        // Set commands
        Command::Sadd {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Set(set) => {
                            let added = set.insert(member.clone());
                            reconcile_entry_size(state, &k);
                            return RespValue::Integer(if added { 1 } else { 0 });
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            // Create new set
            let mut set = HashSet::new();
            set.insert(member.clone());
            let size = calculate_entry_size(&k, &EntryData::Set(set.clone()));
            let entry = Entry {
                data: EntryData::Set(set),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Srem {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::Set(set) => {
                        let removed = set.remove(member);
                        reconcile_entry_size(state, &k);
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Smove {
            ref source,
            ref destination,
            ref member,
        } => {
            let sk = tenant_key(&tenant_id, source);
            let dk = tenant_key(&tenant_id, destination);

            // Remove from source.
            let removed = if let Some(entry) = state.cache.get_mut(&sk) {
                if TenantState::is_expired(entry) {
                    state.remove(&sk);
                    false
                } else if let EntryData::Set(set) = &mut entry.data {
                    let removed = set.remove(member);
                    reconcile_entry_size(state, &sk);
                    removed
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            } else {
                false
            };

            if !removed {
                return RespValue::Integer(0);
            }

            // Add to destination (create if missing).
            if let Some(entry) = state.cache.get_mut(&dk) {
                if TenantState::is_expired(entry) {
                    state.remove(&dk);
                } else if let EntryData::Set(set) = &mut entry.data {
                    set.insert(member.clone());
                    reconcile_entry_size(state, &dk);
                    return RespValue::Integer(1);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            let mut set = HashSet::new();
            set.insert(member.clone());
            let size = calculate_entry_size(&dk, &EntryData::Set(set.clone()));
            let entry = Entry {
                data: EntryData::Set(set),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(dk, entry);
            RespValue::Integer(1)
        }
        Command::Spop { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::BulkString(None);
                }
                match &mut entry.data {
                    EntryData::Set(set) => {
                        if let Some(member) = set.iter().next().cloned() {
                            set.remove(&member);
                            reconcile_entry_size(state, &k);
                            RespValue::BulkString(Some(member.into_bytes()))
                        } else {
                            RespValue::BulkString(None)
                        }
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::BulkString(None)
            }
        }
        Command::Smembers { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::Set(set) => {
                            let members: Vec<RespValue> = set
                                .iter()
                                .map(|m| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(members)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        // Sorted Set commands
        Command::Zadd {
            ref key,
            score,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::ZSet(zset) => {
                            let is_new = zset.insert(member.clone(), score).is_none();
                            reconcile_entry_size(state, &k);
                            return RespValue::Integer(if is_new { 1 } else { 0 });
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            // Create new zset
            let mut zset = BTreeMap::new();
            zset.insert(member.clone(), score);
            let size = calculate_entry_size(&k, &EntryData::ZSet(zset.clone()));
            let entry = Entry {
                data: EntryData::ZSet(zset),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Zincrby {
            ref key,
            delta,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else {
                    match &mut entry.data {
                        EntryData::ZSet(zset) => {
                            let current = zset.get(member).copied().unwrap_or(0.0);
                            let next = current + delta;
                            zset.insert(member.clone(), next);
                            reconcile_entry_size(state, &k);
                            return RespValue::BulkString(Some(next.to_string().into_bytes()));
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    }
                }
            }

            let mut zset = BTreeMap::new();
            zset.insert(member.clone(), delta);
            let size = calculate_entry_size(&k, &EntryData::ZSet(zset.clone()));
            let entry = Entry {
                data: EntryData::ZSet(zset),
                expires_at: None,
                size,
            };
            state.used_bytes = state.used_bytes.saturating_add(size);
            state.cache.put(k, entry);
            RespValue::BulkString(Some(delta.to_string().into_bytes()))
        }
        Command::Zrangebyscore {
            ref key,
            ref min,
            ref max,
        } => {
            let k = tenant_key(&tenant_id, key);
            let (min_v, min_excl) = parse_zbound(min);
            let (max_v, max_excl) = parse_zbound(max);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            let mut members: Vec<(&String, &f64)> = zset.iter().collect();
                            members.sort_by(|a, b| {
                                a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let result: Vec<RespValue> = members
                                .iter()
                                .filter(|(_, score)| {
                                    let above = if min_excl {
                                        **score > min_v
                                    } else {
                                        **score >= min_v
                                    };
                                    let below = if max_excl {
                                        **score < max_v
                                    } else {
                                        **score <= max_v
                                    };
                                    above && below
                                })
                                .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(result)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        Command::Zremrangebyscore {
            ref key,
            ref min,
            ref max,
        } => {
            let k = tenant_key(&tenant_id, key);
            let (min_v, min_excl) = parse_zbound(min);
            let (max_v, max_excl) = parse_zbound(max);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::ZSet(zset) => {
                        let to_remove: Vec<String> = zset
                            .iter()
                            .filter(|(_, score)| {
                                let above = if min_excl {
                                    **score > min_v
                                } else {
                                    **score >= min_v
                                };
                                let below = if max_excl {
                                    **score < max_v
                                } else {
                                    **score <= max_v
                                };
                                above && below
                            })
                            .map(|(m, _)| m.clone())
                            .collect();
                        let count = to_remove.len() as i64;
                        for m in to_remove {
                            zset.remove(&m);
                        }
                        reconcile_entry_size(state, &k);
                        RespValue::Integer(count)
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Zcount {
            ref key,
            ref min,
            ref max,
        } => {
            let k = tenant_key(&tenant_id, key);
            let (min_v, min_excl) = parse_zbound(min);
            let (max_v, max_excl) = parse_zbound(max);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Integer(0);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            let count = zset
                                .values()
                                .filter(|score| {
                                    let above = if min_excl {
                                        **score > min_v
                                    } else {
                                        **score >= min_v
                                    };
                                    let below = if max_excl {
                                        **score < max_v
                                    } else {
                                        **score <= max_v
                                    };
                                    above && below
                                })
                                .count() as i64;
                            RespValue::Integer(count)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Zrem {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::ZSet(zset) => {
                        let removed = zset.remove(member).is_some();
                        reconcile_entry_size(state, &k);
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Zrange {
            ref key,
            start,
            stop,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            // Collect and sort by score
                            let mut members: Vec<(&String, &f64)> = zset.iter().collect();
                            members.sort_by(|a, b| {
                                a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            let len = members.len() as i64;
                            let start_idx = if start < 0 {
                                (len + start).max(0)
                            } else {
                                start.min(len)
                            } as usize;
                            let stop_idx = if stop < 0 {
                                (len + stop + 1).max(0)
                            } else {
                                (stop + 1).min(len)
                            } as usize;

                            let result: Vec<RespValue> = members
                                [start_idx..stop_idx.min(members.len())]
                                .iter()
                                .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(result)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        Command::Zcard { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Integer(0)
                    } else {
                        match &entry.data {
                            EntryData::ZSet(zset) => RespValue::Integer(zset.len() as i64),
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Zscore {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::BulkString(None)
                    } else {
                        match &entry.data {
                            EntryData::ZSet(zset) => {
                                if let Some(score) = zset.get(member) {
                                    RespValue::BulkString(Some(score.to_string().into_bytes()))
                                } else {
                                    RespValue::BulkString(None)
                                }
                            }
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        // Hash extensions
        Command::Hgetall { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Array(vec![])
                    } else {
                        match &entry.data {
                            EntryData::Hash(hash) => {
                                let mut result = Vec::new();
                                for (field, value) in hash {
                                    result.push(RespValue::BulkString(Some(
                                        field.as_bytes().to_vec(),
                                    )));
                                    result.push(RespValue::BulkString(Some(value.clone())));
                                }
                                RespValue::Array(result)
                            }
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        Command::Hkeys { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Array(vec![])
                    } else {
                        match &entry.data {
                            EntryData::Hash(hash) => {
                                let keys: Vec<RespValue> = hash
                                    .keys()
                                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                                    .collect();
                                RespValue::Array(keys)
                            }
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        Command::Hvals { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Array(vec![])
                    } else {
                        match &entry.data {
                            EntryData::Hash(hash) => {
                                let values: Vec<RespValue> = hash
                                    .values()
                                    .map(|v| RespValue::BulkString(Some(v.clone())))
                                    .collect();
                                RespValue::Array(values)
                            }
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        // Set extensions
        Command::Scard { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Integer(0)
                    } else {
                        match &entry.data {
                            EntryData::Set(set) => RespValue::Integer(set.len() as i64),
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Sismember {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Integer(0)
                    } else {
                        match &entry.data {
                            EntryData::Set(set) => {
                                RespValue::Integer(if set.contains(member) { 1 } else { 0 })
                            }
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        // List commands
        Command::Lpush {
            ref key,
            ref values,
        } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else if let EntryData::List(list) = &mut entry.data {
                    for value in values.iter().rev() {
                        list.push_front(value.clone());
                        let added = (k.len() + value.len()) as u64;
                        entry.size = entry.size.saturating_add(added);
                        state.used_bytes = state.used_bytes.saturating_add(added);
                    }
                    return RespValue::Integer(list.len() as i64);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }

            // Create new list
            let mut list = VecDeque::new();
            for value in values.iter() {
                list.push_front(value.clone());
            }
            let entry_size =
                calculate_entry_size(&k, &EntryData::List(list.iter().cloned().collect()));
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }

            let entry = Entry {
                data: EntryData::List(list),
                expires_at: None,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::Integer(values.len() as i64)
        }
        Command::Rpush {
            ref key,
            ref values,
        } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else if let EntryData::List(list) = &mut entry.data {
                    for value in values.iter() {
                        list.push_back(value.clone());
                        let added = (k.len() + value.len()) as u64;
                        entry.size = entry.size.saturating_add(added);
                        state.used_bytes = state.used_bytes.saturating_add(added);
                    }
                    return RespValue::Integer(list.len() as i64);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }

            // Create new list
            let mut list = VecDeque::new();
            for value in values.iter() {
                list.push_back(value.clone());
            }
            let entry_size =
                calculate_entry_size(&k, &EntryData::List(list.iter().cloned().collect()));
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }

            let entry = Entry {
                data: EntryData::List(list),
                expires_at: None,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::Integer(values.len() as i64)
        }
        Command::Lpushx {
            ref key,
            ref values,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if let EntryData::List(list) = &mut entry.data {
                    for value in values.iter().rev() {
                        list.push_front(value.clone());
                        let added = (k.len() + value.len()) as u64;
                        entry.size = entry.size.saturating_add(added);
                        state.used_bytes = state.used_bytes.saturating_add(added);
                    }
                    return RespValue::Integer(list.len() as i64);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            RespValue::Integer(0)
        }
        Command::Rpushx {
            ref key,
            ref values,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if let EntryData::List(list) = &mut entry.data {
                    for value in values.iter() {
                        list.push_back(value.clone());
                        let added = (k.len() + value.len()) as u64;
                        entry.size = entry.size.saturating_add(added);
                        state.used_bytes = state.used_bytes.saturating_add(added);
                    }
                    return RespValue::Integer(list.len() as i64);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            RespValue::Integer(0)
        }
        Command::Lpop { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::BulkString(None);
                }
                match &mut entry.data {
                    EntryData::List(list) => {
                        if let Some(value) = list.pop_front() {
                            RespValue::BulkString(Some(value))
                        } else {
                            RespValue::BulkString(None)
                        }
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::BulkString(None)
            }
        }
        Command::Rpop { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::BulkString(None);
                }
                match &mut entry.data {
                    EntryData::List(list) => {
                        if let Some(value) = list.pop_back() {
                            RespValue::BulkString(Some(value))
                        } else {
                            RespValue::BulkString(None)
                        }
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::BulkString(None)
            }
        }
        Command::Llen { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Integer(0)
                    } else {
                        match &entry.data {
                            EntryData::List(list) => RespValue::Integer(list.len() as i64),
                            _ => RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ),
                        }
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Lrange {
            ref key,
            start,
            stop,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::List(list) => {
                            let len = list.len() as i64;
                            let start_idx = if start < 0 {
                                (len + start).max(0)
                            } else {
                                start.min(len)
                            } as usize;
                            let stop_idx = if stop < 0 {
                                (len + stop + 1).max(0)
                            } else {
                                (stop + 1).min(len)
                            } as usize;

                            let result: Vec<RespValue> = list
                                .iter()
                                .skip(start_idx)
                                .take(stop_idx.saturating_sub(start_idx))
                                .map(|v| RespValue::BulkString(Some(v.clone())))
                                .collect();
                            RespValue::Array(result)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        Command::Lindex { ref key, index } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::BulkString(None);
                    }
                    match &entry.data {
                        EntryData::List(list) => {
                            let len = list.len() as i64;
                            let idx = if index < 0 { len + index } else { index };
                            if idx < 0 || idx >= len {
                                RespValue::BulkString(None)
                            } else {
                                RespValue::BulkString(Some(list[idx as usize].clone()))
                            }
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Lset {
            ref key,
            index,
            ref value,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Error("ERR no such key".to_string());
                }
                match &mut entry.data {
                    EntryData::List(list) => {
                        let len = list.len() as i64;
                        let idx = if index < 0 { len + index } else { index };
                        if idx < 0 || idx >= len {
                            return RespValue::Error("ERR index out of range".to_string());
                        }
                        let idx = idx as usize;
                        let old = &list[idx];
                        let delta = (value.len() as i128) - (old.len() as i128);
                        list[idx] = value.to_vec();
                        entry.size = (entry.size as i128 + delta).max(0) as u64;
                        if delta >= 0 {
                            state.used_bytes = state.used_bytes.saturating_add(delta as u64);
                        } else {
                            state.used_bytes = state.used_bytes.saturating_sub((-delta) as u64);
                        }
                        RespValue::SimpleString("OK".to_string())
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Error("ERR no such key".to_string())
            }
        }
        Command::Ltrim {
            ref key,
            start,
            stop,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::SimpleString("OK".to_string());
                }
                match &mut entry.data {
                    EntryData::List(list) => {
                        let len = list.len() as i64;
                        let start_idx = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        } as usize;
                        let stop_idx = if stop < 0 {
                            (len + stop + 1).max(0)
                        } else {
                            (stop + 1).min(len)
                        } as usize;
                        let kept: VecDeque<Vec<u8>> = list
                            .iter()
                            .skip(start_idx)
                            .take(stop_idx.saturating_sub(start_idx))
                            .cloned()
                            .collect();
                        let new_size = calculate_entry_size(&k, &EntryData::List(kept.clone()));
                        let delta = new_size as i128 - entry.size as i128;
                        entry.data = EntryData::List(kept);
                        entry.size = new_size;
                        if delta >= 0 {
                            state.used_bytes = state.used_bytes.saturating_add(delta as u64);
                        } else {
                            state.used_bytes = state.used_bytes.saturating_sub((-delta) as u64);
                        }
                        RespValue::SimpleString("OK".to_string())
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::SimpleString("OK".to_string())
            }
        }
        Command::Linsert {
            ref key,
            before,
            ref pivot,
            ref value,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(-1);
                }
                match &mut entry.data {
                    EntryData::List(list) => {
                        if let Some(pos) = list.iter().position(|v| v == pivot) {
                            let insert_at = if before { pos } else { pos + 1 };
                            list.insert(insert_at, value.to_vec());
                            let new_size = calculate_entry_size(
                                &k,
                                &EntryData::List(list.iter().cloned().collect()),
                            );
                            let delta = new_size as i128 - entry.size as i128;
                            entry.size = new_size;
                            if delta >= 0 {
                                state.used_bytes = state.used_bytes.saturating_add(delta as u64);
                            } else {
                                state.used_bytes = state.used_bytes.saturating_sub((-delta) as u64);
                            }
                            RespValue::Integer(list.len() as i64)
                        } else {
                            RespValue::Integer(-1)
                        }
                    }
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        // String extensions
        Command::Incr { ref key } => incr_decr(state, &tenant_id, key, 1),
        Command::Decr { ref key } => incr_decr(state, &tenant_id, key, -1),
        Command::Incrby { ref key, delta } => incr_decr(state, &tenant_id, key, delta),
        Command::Decrby { ref key, delta } => incr_decr(state, &tenant_id, key, -delta),
        Command::Append { ref key, ref value } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else if let EntryData::String(bytes) = &mut entry.data {
                    let added = value.len() as u64;
                    bytes.extend_from_slice(value);
                    entry.size = entry.size.saturating_add(added);
                    state.used_bytes = state.used_bytes.saturating_add(added);
                    return RespValue::Integer(bytes.len() as i64);
                } else {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            // Key did not exist: create it.
            let entry_size = entry_size_bytes(&k, value);
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }
            let entry = Entry {
                data: EntryData::String(value.to_vec()),
                expires_at: None,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::Integer(value.len() as i64)
        }
        Command::Exists { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::Integer(0)
                    } else {
                        RespValue::Integer(1)
                    }
                }
                None => RespValue::Integer(0),
            }
        }
        Command::Type { ref key } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        RespValue::SimpleString("none".to_string())
                    } else {
                        let t = match &entry.data {
                            EntryData::String(_) => "string",
                            EntryData::Hash(_) => "hash",
                            EntryData::Set(_) => "set",
                            EntryData::ZSet(_) => "zset",
                            EntryData::List(_) => "list",
                        };
                        RespValue::SimpleString(t.to_string())
                    }
                }
                None => RespValue::SimpleString("none".to_string()),
            }
        }
        Command::Persist { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if entry.expires_at.is_some() {
                    entry.expires_at = None;
                    RespValue::Integer(1)
                } else {
                    RespValue::Integer(0)
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Pttl { ref key } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(-2);
                }
                if let Some(expires_at) = entry.expires_at {
                    let now = current_timestamp();
                    let ttl_ms = ((expires_at as i64) - (now as i64)) * 1000;
                    RespValue::Integer(ttl_ms.max(0))
                } else {
                    RespValue::Integer(-1)
                }
            } else {
                RespValue::Integer(-2)
            }
        }
        Command::Pexpire {
            ref key,
            milliseconds,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if milliseconds <= 0 {
                    state.remove(&k);
                    return RespValue::Integer(1);
                }
                entry.expires_at = Some(current_timestamp() + (milliseconds as u64).div_ceil(1000));
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Pexpireat {
            ref key,
            milliseconds_timestamp,
        } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if milliseconds_timestamp <= 0 {
                    state.remove(&k);
                    return RespValue::Integer(1);
                }
                entry.expires_at = Some((milliseconds_timestamp as u64).div_ceil(1000));
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Expireat { ref key, timestamp } => {
            let k = tenant_key(&tenant_id, key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                    return RespValue::Integer(0);
                }
                if timestamp <= 0 {
                    state.remove(&k);
                    return RespValue::Integer(1);
                }
                entry.expires_at = Some(timestamp as u64);
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        // Bulk / multi-key commands
        Command::Mset { ref pairs } => {
            for (key, value) in pairs {
                let k = tenant_key(&tenant_id, key);
                let entry_size = entry_size_bytes(&k, value);
                if entry_size > state.limit_bytes {
                    return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                }
                if let Some(existing) = state.cache.pop(&k) {
                    state.used_bytes = state.used_bytes.saturating_sub(existing.size);
                }
                let mut projected = state.used_bytes + entry_size;
                while projected > state.limit_bytes {
                    if let Some((_ek, ev)) = state.cache.pop_lru() {
                        state.used_bytes = state.used_bytes.saturating_sub(ev.size);
                        state.record_eviction();
                        projected = state.used_bytes + entry_size;
                    } else {
                        return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                    }
                }
                let entry = Entry {
                    data: EntryData::String(value.clone()),
                    expires_at: None,
                    size: entry_size,
                };
                state.cache.put(k, entry);
                state.used_bytes = state.used_bytes.saturating_add(entry_size);
            }
            RespValue::SimpleString("OK".to_string())
        }
        Command::Mget { ref keys } => {
            let mut result = Vec::with_capacity(keys.len());
            for key in keys {
                let k = tenant_key(&tenant_id, key);
                match state.cache.get(&k) {
                    Some(entry) => {
                        if TenantState::is_expired(entry) {
                            state.remove(&k);
                            result.push(RespValue::BulkString(None));
                        } else if let EntryData::String(bytes) = &entry.data {
                            result.push(RespValue::BulkString(Some(bytes.clone())));
                        } else {
                            result.push(RespValue::BulkString(None));
                        }
                    }
                    None => result.push(RespValue::BulkString(None)),
                }
            }
            RespValue::Array(result)
        }
        Command::Keys { ref pattern } => {
            let re = match glob_to_regex(pattern) {
                Ok(re) => re,
                Err(_) => return RespValue::Error("ERR invalid pattern".to_string()),
            };
            let mut result = Vec::new();
            for (k, entry) in state.cache.iter() {
                if TenantState::is_expired(entry) {
                    continue;
                }
                // Strip the tenant prefix for user-facing keys.
                let raw = match k.split_once(':') {
                    Some((_, raw)) => raw,
                    None => k.as_str(),
                };
                if re.is_match(raw) {
                    result.push(RespValue::BulkString(Some(raw.as_bytes().to_vec())));
                }
            }
            RespValue::Array(result)
        }
        Command::Flushdb => {
            state.cache.clear();
            state.used_bytes = 0;
            RespValue::SimpleString("OK".to_string())
        }
        Command::Rename { ref from, ref to } => {
            let k_from = tenant_key(&tenant_id, from);
            let k_to = tenant_key(&tenant_id, to);
            if let Some(mut entry) = state.cache.pop(&k_from) {
                if TenantState::is_expired(&entry) {
                    return RespValue::Error("ERR no such key".to_string());
                }
                state.used_bytes = state.used_bytes.saturating_sub(entry.size);
                if let Some(existing) = state.cache.pop(&k_to) {
                    state.used_bytes = state.used_bytes.saturating_sub(existing.size);
                }
                entry.size = calculate_entry_size(&k_to, &entry.data);
                state.used_bytes = state.used_bytes.saturating_add(entry.size);
                state.cache.put(k_to, entry);
                RespValue::SimpleString("OK".to_string())
            } else {
                RespValue::Error("ERR no such key".to_string())
            }
        }
        Command::Dbsize => {
            let count = state
                .cache
                .iter()
                .filter(|(_, entry)| !TenantState::is_expired(entry))
                .count() as i64;
            RespValue::Integer(count)
        }
        Command::Randomkey => {
            // Pick a non-expired key deterministically-ish via the first entry
            // after a rotating cursor. Good enough for the random-key use case.
            let key = state
                .cache
                .iter()
                .find(|(_, entry)| !TenantState::is_expired(entry))
                .map(|(k, _)| k.clone());
            match key {
                Some(k) => {
                    // Strip the tenant prefix for the returned key.
                    let raw = k.split_once(':').map(|(_, r)| r).unwrap_or(&k);
                    RespValue::BulkString(Some(raw.as_bytes().to_vec()))
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Echo { ref message } => RespValue::BulkString(Some(message.as_bytes().to_vec())),
        Command::Info => {
            let mut info = String::new();
            info.push_str("# Server\r\n");
            info.push_str("ultracache_version:1.0.0\r\n");
            info.push_str("mode:standalone\r\n");
            info.push_str(&format!("tenant_id:{}\r\n", tenant_id));
            info.push_str("# Clients\r\n");
            info.push_str("connected_clients:0\r\n");
            info.push_str("# Memory\r\n");
            info.push_str(&format!("used_memory:{}\r\n", state.used_bytes));
            info.push_str(&format!("maxmemory:{}\r\n", state.limit_bytes));
            info.push_str("# Stats\r\n");
            info.push_str(&format!("total_commands:{}\r\n", state.total_commands));
            info.push_str(&format!("evicted_keys:{}\r\n", state.eviction_count));
            info.push_str(&format!("latency_p99_micros:{}\r\n", state.calculate_p99()));
            RespValue::BulkString(Some(info.into_bytes()))
        }
        Command::ConfigGet { ref parameter } => {
            let param = parameter.to_lowercase();
            let mut values: Vec<RespValue> = Vec::new();
            if param == "maxmemory" || param == "*" {
                values.push(RespValue::BulkString(Some(b"maxmemory".to_vec())));
                values.push(RespValue::BulkString(Some(
                    state.limit_bytes.to_string().into_bytes(),
                )));
            }
            if param == "maxmemory-policy" || param == "*" {
                values.push(RespValue::BulkString(Some(b"maxmemory-policy".to_vec())));
                values.push(RespValue::BulkString(Some(b"allkeys-lru".to_vec())));
            }
            RespValue::Array(values)
        }
        Command::ConfigSet {
            ref parameter,
            ref value,
        } => {
            let param = parameter.to_lowercase();
            if param == "maxmemory" {
                if let Ok(bytes) = value.parse::<u64>() {
                    state.limit_bytes = bytes;
                    RespValue::SimpleString("OK".to_string())
                } else {
                    RespValue::Error("ERR invalid maxmemory value".to_string())
                }
            } else {
                RespValue::Error(format!("ERR unknown parameter '{parameter}'"))
            }
        }
        // Sorted set extensions
        Command::Zrank {
            ref key,
            ref member,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::BulkString(None);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            let mut members: Vec<(&String, &f64)> = zset.iter().collect();
                            members.sort_by(|a, b| {
                                a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            for (idx, (m, _)) in members.iter().enumerate() {
                                if *m == member {
                                    return RespValue::Integer(idx as i64);
                                }
                            }
                            RespValue::BulkString(None)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Zrevrange {
            ref key,
            start,
            stop,
        } => {
            let k = tenant_key(&tenant_id, key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.remove(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            let mut members: Vec<(&String, &f64)> = zset.iter().collect();
                            members.sort_by(|a, b| {
                                b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let len = members.len() as i64;
                            let start_idx = if start < 0 {
                                (len + start).max(0)
                            } else {
                                start.min(len)
                            } as usize;
                            let stop_idx = if stop < 0 {
                                (len + stop + 1).max(0)
                            } else {
                                (stop + 1).min(len)
                            } as usize;
                            let result: Vec<RespValue> = members
                                [start_idx..stop_idx.min(members.len())]
                                .iter()
                                .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(result)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        // Set extensions
        Command::Sunion { ref keys } => {
            let mut union: HashSet<String> = HashSet::new();
            for key in keys {
                let k = tenant_key(&tenant_id, key);
                if let Some(entry) = state.cache.get(&k) {
                    if TenantState::is_expired(entry) {
                        continue;
                    }
                    if let EntryData::Set(set) = &entry.data {
                        for m in set {
                            union.insert(m.clone());
                        }
                    }
                }
            }
            let mut members: Vec<RespValue> = union
                .iter()
                .map(|m| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                .collect();
            members.sort_by(|a, b| match (a, b) {
                (RespValue::BulkString(Some(x)), RespValue::BulkString(Some(y))) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            RespValue::Array(members)
        }
        Command::Sdiff { ref keys } => {
            if keys.is_empty() {
                return RespValue::Array(vec![]);
            }
            let first = tenant_key(&tenant_id, &keys[0]);
            let mut diff: HashSet<String> = match state.cache.get(&first) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        HashSet::new()
                    } else if let EntryData::Set(set) = &entry.data {
                        set.clone()
                    } else {
                        return RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                }
                None => HashSet::new(),
            };
            for key in &keys[1..] {
                let k = tenant_key(&tenant_id, key);
                if let Some(entry) = state.cache.get(&k) {
                    if TenantState::is_expired(entry) {
                        continue;
                    }
                    if let EntryData::Set(set) = &entry.data {
                        for m in set {
                            diff.remove(m);
                        }
                    }
                }
            }
            let mut members: Vec<RespValue> = diff
                .iter()
                .map(|m| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                .collect();
            members.sort_by(|a, b| match (a, b) {
                (RespValue::BulkString(Some(x)), RespValue::BulkString(Some(y))) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            RespValue::Array(members)
        }
    };

    // Persist mutating commands to the AOF for crash recovery. We only log
    // commands that actually changed state (no error responses) so a replay
    // reproduces the exact final state.
    if let Some(aof) = persistence
        && is_mutating_command(&command)
        && !matches!(result, RespValue::Error(_))
        && let Some(args) = command_to_args(&command)
    {
        let _ = aof.log_command(&tenant_id, &args);
    }

    // Record CPU time and latency
    let elapsed_micros = start.elapsed().as_micros() as u64;
    state.record_cpu_time(elapsed_micros);
    state.record_latency(elapsed_micros);

    result
}

/// Whether a command mutates tenant state and therefore must be persisted.
fn is_mutating_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set { .. }
            | Command::GetSet { .. }
            | Command::Del { .. }
            | Command::Expire { .. }
            | Command::Pexpire { .. }
            | Command::Pexpireat { .. }
            | Command::Expireat { .. }
            | Command::Hset { .. }
            | Command::Hdel { .. }
            | Command::Hincrby { .. }
            | Command::Hincrbyfloat { .. }
            | Command::Hsetnx { .. }
            | Command::Sadd { .. }
            | Command::Srem { .. }
            | Command::Smove { .. }
            | Command::Spop { .. }
            | Command::Zadd { .. }
            | Command::Zrem { .. }
            | Command::Zincrby { .. }
            | Command::Zremrangebyscore { .. }
            | Command::Lpush { .. }
            | Command::Lpushx { .. }
            | Command::Rpush { .. }
            | Command::Rpushx { .. }
            | Command::Lpop { .. }
            | Command::Rpop { .. }
            | Command::Lset { .. }
            | Command::Ltrim { .. }
            | Command::Linsert { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Incrby { .. }
            | Command::Decrby { .. }
            | Command::Append { .. }
            | Command::Persist { .. }
            | Command::Mset { .. }
            | Command::Flushdb
            | Command::Rename { .. }
            | Command::ConfigSet { .. }
    )
}

/// Serialize a command back into RESP bulk-string arguments for AOF logging.
fn command_to_args(cmd: &Command) -> Option<Vec<String>> {
    match cmd {
        Command::Set {
            key,
            value,
            ex,
            px,
            nx,
            xx,
        } => {
            let mut args = vec![
                "SET".to_string(),
                key.clone(),
                String::from_utf8_lossy(value).into_owned(),
            ];
            if let Some(secs) = ex {
                args.push("EX".to_string());
                args.push(secs.to_string());
            }
            if let Some(ms) = px {
                args.push("PX".to_string());
                args.push(ms.to_string());
            }
            if *nx {
                args.push("NX".to_string());
            }
            if *xx {
                args.push("XX".to_string());
            }
            Some(args)
        }
        Command::GetSet { key, value } => Some(vec![
            "GETSET".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::StrLen { key } => Some(vec!["STRLEN".to_string(), key.clone()]),
        Command::Del { key } => Some(vec!["DEL".to_string(), key.clone()]),
        Command::Expire { key, seconds } => {
            Some(vec!["EXPIRE".to_string(), key.clone(), seconds.to_string()])
        }
        Command::Pexpire { key, milliseconds } => Some(vec![
            "PEXPIRE".to_string(),
            key.clone(),
            milliseconds.to_string(),
        ]),
        Command::Pexpireat {
            key,
            milliseconds_timestamp,
        } => Some(vec![
            "PEXPIREAT".to_string(),
            key.clone(),
            milliseconds_timestamp.to_string(),
        ]),
        Command::Expireat { key, timestamp } => Some(vec![
            "EXPIREAT".to_string(),
            key.clone(),
            timestamp.to_string(),
        ]),
        Command::Hset { key, field, value } => Some(vec![
            "HSET".to_string(),
            key.clone(),
            field.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Hdel { key, field } => Some(vec!["HDEL".to_string(), key.clone(), field.clone()]),
        Command::Hincrby { key, field, delta } => Some(vec![
            "HINCRBY".to_string(),
            key.clone(),
            field.clone(),
            delta.to_string(),
        ]),
        Command::Hincrbyfloat { key, field, delta } => Some(vec![
            "HINCRBYFLOAT".to_string(),
            key.clone(),
            field.clone(),
            delta.to_string(),
        ]),
        Command::Hsetnx { key, field, value } => Some(vec![
            "HSETNX".to_string(),
            key.clone(),
            field.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Sadd { key, member } => {
            Some(vec!["SADD".to_string(), key.clone(), member.clone()])
        }
        Command::Srem { key, member } => {
            Some(vec!["SREM".to_string(), key.clone(), member.clone()])
        }
        Command::Smove {
            source,
            destination,
            member,
        } => Some(vec![
            "SMOVE".to_string(),
            source.clone(),
            destination.clone(),
            member.clone(),
        ]),
        Command::Spop { key } => Some(vec!["SPOP".to_string(), key.clone()]),
        Command::Zadd { key, score, member } => Some(vec![
            "ZADD".to_string(),
            key.clone(),
            score.to_string(),
            member.clone(),
        ]),
        Command::Zrem { key, member } => {
            Some(vec!["ZREM".to_string(), key.clone(), member.clone()])
        }
        Command::Zincrby { key, delta, member } => Some(vec![
            "ZINCRBY".to_string(),
            key.clone(),
            delta.to_string(),
            member.clone(),
        ]),
        Command::Zremrangebyscore { key, min, max } => Some(vec![
            "ZREMRANGEBYSCORE".to_string(),
            key.clone(),
            min.clone(),
            max.clone(),
        ]),
        Command::Lpush { key, values } => {
            let mut v = vec!["LPUSH".to_string(), key.clone()];
            for val in values {
                v.push(String::from_utf8_lossy(val).into_owned());
            }
            Some(v)
        }
        Command::Lpushx { key, values } => {
            let mut v = vec!["LPUSHX".to_string(), key.clone()];
            for val in values {
                v.push(String::from_utf8_lossy(val).into_owned());
            }
            Some(v)
        }
        Command::Rpush { key, values } => {
            let mut v = vec!["RPUSH".to_string(), key.clone()];
            for val in values {
                v.push(String::from_utf8_lossy(val).into_owned());
            }
            Some(v)
        }
        Command::Rpushx { key, values } => {
            let mut v = vec!["RPUSHX".to_string(), key.clone()];
            for val in values {
                v.push(String::from_utf8_lossy(val).into_owned());
            }
            Some(v)
        }
        Command::Lpop { key } => Some(vec!["LPOP".to_string(), key.clone()]),
        Command::Rpop { key } => Some(vec!["RPOP".to_string(), key.clone()]),
        Command::Lset { key, index, value } => Some(vec![
            "LSET".to_string(),
            key.clone(),
            index.to_string(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Ltrim { key, start, stop } => Some(vec![
            "LTRIM".to_string(),
            key.clone(),
            start.to_string(),
            stop.to_string(),
        ]),
        Command::Linsert {
            key,
            before,
            pivot,
            value,
        } => Some(vec![
            "LINSERT".to_string(),
            key.clone(),
            if *before { "BEFORE" } else { "AFTER" }.to_string(),
            String::from_utf8_lossy(pivot).into_owned(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Incr { key } => Some(vec!["INCR".to_string(), key.clone()]),
        Command::Decr { key } => Some(vec!["DECR".to_string(), key.clone()]),
        Command::Incrby { key, delta } => {
            Some(vec!["INCRBY".to_string(), key.clone(), delta.to_string()])
        }
        Command::Decrby { key, delta } => {
            Some(vec!["DECRBY".to_string(), key.clone(), delta.to_string()])
        }
        Command::Append { key, value } => Some(vec![
            "APPEND".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Persist { key } => Some(vec!["PERSIST".to_string(), key.clone()]),
        Command::Mset { pairs } => {
            let mut args = vec!["MSET".to_string()];
            for (k, v) in pairs {
                args.push(k.clone());
                args.push(String::from_utf8_lossy(v).into_owned());
            }
            Some(args)
        }
        Command::Flushdb => Some(vec!["FLUSHDB".to_string()]),
        Command::Rename { from, to } => Some(vec!["RENAME".to_string(), from.clone(), to.clone()]),
        Command::ConfigSet { parameter, value } => Some(vec![
            "CONFIG".to_string(),
            "SET".to_string(),
            parameter.clone(),
            value.clone(),
        ]),
        _ => None,
    }
}

fn tenant_key(tenant_id: &str, key: &str) -> String {
    format!("{}:{}", tenant_id, key)
}

/// Parse already-tokenized RESP arguments (as produced by the AOF replay) into
/// a `Command`. Used during startup recovery. Returns `None` for unknown or
/// malformed commands.
fn parse_args_to_command(args: &[String]) -> Option<Command> {
    if args.is_empty() {
        return None;
    }
    let cmd = args[0].to_uppercase();
    let cmd = match cmd.as_str() {
        "SET" if args.len() >= 3 => {
            let key = args[1].clone();
            let value = args[2].as_bytes().to_vec();
            let mut ex: Option<i64> = None;
            let mut px: Option<i64> = None;
            let mut nx = false;
            let mut xx = false;
            let mut i = 3;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" if i + 1 < args.len() => {
                        ex = Some(args[i + 1].parse().ok()?);
                        i += 2;
                    }
                    "PX" if i + 1 < args.len() => {
                        px = Some(args[i + 1].parse().ok()?);
                        i += 2;
                    }
                    "NX" => {
                        nx = true;
                        i += 1;
                    }
                    "XX" => {
                        xx = true;
                        i += 1;
                    }
                    _ => i += 1,
                }
            }
            Command::Set {
                key,
                value,
                ex,
                px,
                nx,
                xx,
            }
        }
        "GETSET" if args.len() == 3 => Command::GetSet {
            key: args[1].clone(),
            value: args[2].as_bytes().to_vec(),
        },
        "DEL" if args.len() == 2 => Command::Del {
            key: args[1].clone(),
        },
        "EXPIRE" if args.len() == 3 => Command::Expire {
            key: args[1].clone(),
            seconds: args[2].parse().ok()?,
        },
        "PEXPIRE" if args.len() == 3 => Command::Pexpire {
            key: args[1].clone(),
            milliseconds: args[2].parse().ok()?,
        },
        "PEXPIREAT" if args.len() == 3 => Command::Pexpireat {
            key: args[1].clone(),
            milliseconds_timestamp: args[2].parse().ok()?,
        },
        "EXPIREAT" if args.len() == 3 => Command::Expireat {
            key: args[1].clone(),
            timestamp: args[2].parse().ok()?,
        },
        "HSET" if args.len() == 4 => Command::Hset {
            key: args[1].clone(),
            field: args[2].clone(),
            value: args[3].as_bytes().to_vec(),
        },
        "HDEL" if args.len() == 3 => Command::Hdel {
            key: args[1].clone(),
            field: args[2].clone(),
        },
        "HINCRBY" if args.len() == 4 => Command::Hincrby {
            key: args[1].clone(),
            field: args[2].clone(),
            delta: args[3].parse().ok()?,
        },
        "HINCRBYFLOAT" if args.len() == 4 => Command::Hincrbyfloat {
            key: args[1].clone(),
            field: args[2].clone(),
            delta: args[3].parse().ok()?,
        },
        "HSETNX" if args.len() == 4 => Command::Hsetnx {
            key: args[1].clone(),
            field: args[2].clone(),
            value: args[3].as_bytes().to_vec(),
        },
        "SADD" if args.len() == 3 => Command::Sadd {
            key: args[1].clone(),
            member: args[2].clone(),
        },
        "SREM" if args.len() == 3 => Command::Srem {
            key: args[1].clone(),
            member: args[2].clone(),
        },
        "SMOVE" if args.len() == 4 => Command::Smove {
            source: args[1].clone(),
            destination: args[2].clone(),
            member: args[3].clone(),
        },
        "SPOP" if args.len() == 2 => Command::Spop {
            key: args[1].clone(),
        },
        "ZADD" if args.len() == 4 => Command::Zadd {
            key: args[1].clone(),
            score: args[2].parse().ok()?,
            member: args[3].clone(),
        },
        "ZREM" if args.len() == 3 => Command::Zrem {
            key: args[1].clone(),
            member: args[2].clone(),
        },
        "ZINCRBY" if args.len() == 4 => Command::Zincrby {
            key: args[1].clone(),
            delta: args[2].parse().ok()?,
            member: args[3].clone(),
        },
        "ZREMRANGEBYSCORE" if args.len() == 4 => Command::Zremrangebyscore {
            key: args[1].clone(),
            min: args[2].clone(),
            max: args[3].clone(),
        },
        "LPUSH" if args.len() >= 3 => Command::Lpush {
            key: args[1].clone(),
            values: args[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
        },
        "RPUSH" if args.len() >= 3 => Command::Rpush {
            key: args[1].clone(),
            values: args[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
        },
        "LPUSHX" if args.len() >= 3 => Command::Lpushx {
            key: args[1].clone(),
            values: args[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
        },
        "RPUSHX" if args.len() >= 3 => Command::Rpushx {
            key: args[1].clone(),
            values: args[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
        },
        "LPOP" if args.len() == 2 => Command::Lpop {
            key: args[1].clone(),
        },
        "RPOP" if args.len() == 2 => Command::Rpop {
            key: args[1].clone(),
        },
        "LSET" if args.len() == 4 => Command::Lset {
            key: args[1].clone(),
            index: args[2].parse().ok()?,
            value: args[3].as_bytes().to_vec(),
        },
        "LTRIM" if args.len() == 4 => Command::Ltrim {
            key: args[1].clone(),
            start: args[2].parse().ok()?,
            stop: args[3].parse().ok()?,
        },
        "LINSERT" if args.len() == 5 => Command::Linsert {
            key: args[1].clone(),
            before: args[2].eq_ignore_ascii_case("BEFORE"),
            pivot: args[3].as_bytes().to_vec(),
            value: args[4].as_bytes().to_vec(),
        },
        "INCR" if args.len() == 2 => Command::Incr {
            key: args[1].clone(),
        },
        "DECR" if args.len() == 2 => Command::Decr {
            key: args[1].clone(),
        },
        "INCRBY" if args.len() == 3 => Command::Incrby {
            key: args[1].clone(),
            delta: args[2].parse().ok()?,
        },
        "DECRBY" if args.len() == 3 => Command::Decrby {
            key: args[1].clone(),
            delta: args[2].parse().ok()?,
        },
        "APPEND" if args.len() == 3 => Command::Append {
            key: args[1].clone(),
            value: args[2].as_bytes().to_vec(),
        },
        "PERSIST" if args.len() == 2 => Command::Persist {
            key: args[1].clone(),
        },
        "MSET" if args.len() >= 3 && args.len() % 2 == 1 => {
            let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
            let mut idx = 1;
            while idx + 1 < args.len() {
                pairs.push((args[idx].clone(), args[idx + 1].as_bytes().to_vec()));
                idx += 2;
            }
            Command::Mset { pairs }
        }
        "FLUSHDB" if args.len() == 1 => Command::Flushdb,
        "RENAME" if args.len() == 3 => Command::Rename {
            from: args[1].clone(),
            to: args[2].clone(),
        },
        _ => return None,
    };
    Some(cmd)
}

pub struct RuntimeHandle {
    inner: Arc<ShardRuntime>,
}

impl RuntimeHandle {
    pub fn new(shards: usize) -> Self {
        Self {
            inner: Arc::new(ShardRuntime::new(shards)),
        }
    }

    /// Create a runtime that persists mutating commands to the given AOF manager.
    pub fn with_persistence(shards: usize, persistence: Option<Arc<AofManager>>) -> Self {
        Self {
            inner: Arc::new(ShardRuntime::with_persistence(shards, persistence)),
        }
    }

    /// Replay persisted AOF commands into the shards. See `ShardRuntime::recover`.
    pub async fn recover(&self) -> Result<(), String> {
        self.inner.recover().await
    }

    pub fn inner(&self) -> Arc<ShardRuntime> {
        Arc::clone(&self.inner)
    }
}

struct StatsAggregate {
    shards: u64,
    memory_used_bytes: u64,
    memory_limit_bytes: u64,
    cpu_used_micros: u64,
    cpu_quota_micros: u64,
    total_commands: u64,
    eviction_count: u64,
    key_count: u64,
    latency_p99_micros: u64,
}

impl StatsAggregate {
    fn new(shards: u64) -> Self {
        Self {
            shards,
            memory_used_bytes: 0,
            memory_limit_bytes: 0,
            cpu_used_micros: 0,
            cpu_quota_micros: 0,
            total_commands: 0,
            eviction_count: 0,
            key_count: 0,
            latency_p99_micros: 0,
        }
    }

    fn absorb(&mut self, parsed: &HashMap<String, String>) {
        self.memory_used_bytes += parse_u64(parsed.get("memory_used_bytes"));
        self.memory_limit_bytes += parse_u64(parsed.get("memory_limit_bytes"));
        self.cpu_used_micros += parse_u64(parsed.get("cpu_used_micros"));
        self.cpu_quota_micros += parse_u64(parsed.get("cpu_quota_micros"));
        self.total_commands += parse_u64(parsed.get("total_commands"));
        self.eviction_count += parse_u64(parsed.get("eviction_count"));
        self.key_count += parse_u64(parsed.get("key_count"));
        let p99 = parse_u64(parsed.get("latency_p99_micros"));
        if p99 > self.latency_p99_micros {
            self.latency_p99_micros = p99;
        }
    }

    fn format(&self, tenant_id: &str) -> String {
        let memory_usage_pct = if self.memory_limit_bytes > 0 {
            (self.memory_used_bytes as f64 / self.memory_limit_bytes as f64) * 100.0
        } else {
            0.0
        };
        let cpu_usage_pct = if self.cpu_quota_micros > 0 {
            (self.cpu_used_micros as f64 / self.cpu_quota_micros as f64) * 100.0
        } else {
            0.0
        };

        format!(
            "tenant_id: {}\n\
             shards: {}\n\
             memory_used_bytes: {}\n\
             memory_limit_bytes: {}\n\
             memory_usage_pct: {:.2}\n\
             cpu_used_micros: {}\n\
             cpu_quota_micros: {}\n\
             cpu_usage_pct: {:.2}\n\
             total_commands: {}\n\
             eviction_count: {}\n\
             key_count: {}\n\
             latency_p99_micros: {}\n\
             latency_p99_ms: {:.3}",
            tenant_id,
            self.shards,
            self.memory_used_bytes,
            self.memory_limit_bytes,
            memory_usage_pct,
            self.cpu_used_micros,
            self.cpu_quota_micros,
            cpu_usage_pct,
            self.total_commands,
            self.eviction_count,
            self.key_count,
            self.latency_p99_micros,
            self.latency_p99_micros as f64 / 1000.0
        )
    }
}

fn parse_stats_kv(stats: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in stats.lines() {
        if let Some((key, value)) = line.split_once(':') {
            map.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    map
}

fn parse_u64(value: Option<&String>) -> u64 {
    value.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

struct Entry {
    data: EntryData,
    expires_at: Option<u64>,
    /// Cached byte size of this entry (key + value). Kept in sync on every
    /// mutation so tenant memory accounting stays accurate for all data types.
    size: u64,
}

/// Recompute the cached size of an entry in place and adjust the tenant's
/// running `used_bytes` total by the delta. This fixes the previous bug where
/// Hash/Set/ZSet/List mutations after creation never updated memory accounting.
fn reconcile_entry_size(state: &mut TenantState, k: &str) {
    if let Some(entry) = state.cache.get_mut(k) {
        let new_size = calculate_entry_size(k, &entry.data);
        let delta = new_size as i128 - entry.size as i128;
        entry.size = new_size;
        if delta >= 0 {
            state.used_bytes = state.used_bytes.saturating_add(delta as u64);
        } else {
            state.used_bytes = state.used_bytes.saturating_sub((-delta) as u64);
        }
    }
}

#[derive(Clone)]
enum EntryData {
    String(Vec<u8>),
    Hash(std::collections::HashMap<String, Vec<u8>>),
    Set(std::collections::HashSet<String>),
    ZSet(std::collections::BTreeMap<String, f64>),
    List(VecDeque<Vec<u8>>),
}

struct TenantState {
    cache: LruCache<String, Entry>,
    used_bytes: u64,
    limit_bytes: u64,
    cpu_used_micros: u64,
    cpu_quota_micros: u64,
    last_reset: std::time::Instant,
    // Latency tracking (simple histogram)
    latency_samples: Vec<u64>, // Store last N latencies in microseconds
    total_commands: u64,
    eviction_count: u64,
}

impl TenantState {
    fn new(limit_bytes: u64, cpu_quota_micros: u64) -> Self {
        let cap = NonZeroUsize::new(1_000_000).unwrap();
        Self {
            cache: LruCache::new(cap),
            used_bytes: 0,
            limit_bytes,
            cpu_used_micros: 0,
            cpu_quota_micros,
            last_reset: std::time::Instant::now(),
            latency_samples: Vec::with_capacity(1000),
            total_commands: 0,
            eviction_count: 0,
        }
    }

    fn check_and_reset_cpu_budget(&mut self) {
        let elapsed = self.last_reset.elapsed();
        if elapsed.as_secs() >= 1 {
            self.cpu_used_micros = 0;
            self.last_reset = std::time::Instant::now();
        }
    }

    fn is_throttled(&self) -> bool {
        self.cpu_used_micros >= self.cpu_quota_micros
    }

    fn record_cpu_time(&mut self, micros: u64) {
        self.cpu_used_micros = self.cpu_used_micros.saturating_add(micros);
    }

    fn record_latency(&mut self, micros: u64) {
        self.total_commands += 1;
        // Keep last 1000 samples for p99 calculation
        if self.latency_samples.len() >= 1000 {
            self.latency_samples.remove(0);
        }
        self.latency_samples.push(micros);
    }

    fn calculate_p99(&self) -> u64 {
        if self.latency_samples.is_empty() {
            return 0;
        }
        let mut sorted = self.latency_samples.clone();
        sorted.sort_unstable();
        let idx = (sorted.len() as f64 * 0.99).ceil() as usize - 1;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn record_eviction(&mut self) {
        self.eviction_count += 1;
    }

    /// Remove an entry from the cache and adjust `used_bytes` by its cached
    /// size. Used for both explicit deletes and lazy expiry eviction so memory
    /// accounting never leaks.
    fn remove(&mut self, k: &str) -> Option<Entry> {
        if let Some(entry) = self.cache.pop(k) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.size);
            Some(entry)
        } else {
            None
        }
    }

    fn is_expired(entry: &Entry) -> bool {
        if let Some(expires_at) = entry.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            now >= expires_at
        } else {
            false
        }
    }
}

fn entry_size_bytes(key: &str, value: &[u8]) -> u64 {
    (key.len() + value.len()) as u64
}

fn calculate_entry_size(key: &str, data: &EntryData) -> u64 {
    let key_size = key.len() as u64;
    let data_size = match data {
        EntryData::String(bytes) => bytes.len() as u64,
        EntryData::Hash(map) => map
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum::<u64>(),
        EntryData::Set(set) => set.iter().map(|s| s.len() as u64).sum::<u64>(),
        EntryData::ZSet(map) => map
            .keys()
            .map(|member| member.len() as u64 + 8)
            .sum::<u64>(),
        EntryData::List(list) => list.iter().map(|v| v.len() as u64).sum::<u64>(),
    };
    key_size + data_size
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Increment or decrement a string-valued key interpreted as an integer.
/// Used by INCR/DECR/INCRBY/DECRBY. Creates the key at 0 if it does not exist.
fn incr_decr(state: &mut TenantState, tenant_id: &str, key: &str, delta: i64) -> RespValue {
    let k = tenant_key(tenant_id, key);
    if let Some(entry) = state.cache.get_mut(&k) {
        if TenantState::is_expired(entry) {
            state.remove(&k);
        } else if let EntryData::String(bytes) = &mut entry.data {
            let current = match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(num) => num,
                None => {
                    return RespValue::Error("ERR value is not an integer".to_string());
                }
            };
            let next = current.saturating_add(delta);
            let encoded = next.to_string().into_bytes();
            let new_size = calculate_entry_size(&k, &EntryData::String(encoded.clone()));
            let size_delta = new_size as i128 - entry.size as i128;
            entry.data = EntryData::String(encoded);
            entry.size = new_size;
            if size_delta >= 0 {
                state.used_bytes = state.used_bytes.saturating_add(size_delta as u64);
            } else {
                state.used_bytes = state.used_bytes.saturating_sub((-size_delta) as u64);
            }
            return RespValue::Integer(next);
        } else {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        }
    }

    // Key does not exist: start from 0 + delta.
    let next = delta;
    let encoded = next.to_string().into_bytes();
    let entry_size = calculate_entry_size(&k, &EntryData::String(encoded.clone()));
    if entry_size > state.limit_bytes {
        return RespValue::Error("ERR tenant memory limit exceeded".to_string());
    }
    let entry = Entry {
        data: EntryData::String(encoded),
        expires_at: None,
        size: entry_size,
    };
    state.cache.put(k, entry);
    state.used_bytes = state.used_bytes.saturating_add(entry_size);
    RespValue::Integer(next)
}

/// Convert a Redis-style glob pattern (`*` and `?`) into a simple matcher.
/// Avoids pulling in a regex dependency. Returns an error for empty patterns.
fn glob_to_regex(pattern: &str) -> Result<GlobMatcher, ()> {
    if pattern.is_empty() {
        return Err(());
    }
    Ok(GlobMatcher {
        pattern: pattern.to_string(),
    })
}

struct GlobMatcher {
    pattern: String,
}

impl GlobMatcher {
    fn is_match(&self, s: &str) -> bool {
        // Iterative backtracking match supporting '*' (any sequence) and
        // '?' (any single char). Escaped chars via backslash are literal.
        let pat: Vec<char> = self.pattern.chars().collect();
        let txt: Vec<char> = s.chars().collect();
        let (mut pi, mut ti) = (0usize, 0usize);
        let (mut star, mut mark) = (None::<usize>, 0usize);

        while ti < txt.len() {
            if pi < pat.len() && (pat[pi] == '?' || pat[pi] == txt[ti]) {
                pi += 1;
                ti += 1;
            } else if pi < pat.len() && pat[pi] == '*' {
                star = Some(pi);
                mark = ti;
                pi += 1;
            } else if let Some(sp) = star {
                pi = sp + 1;
                mark += 1;
                ti = mark;
            } else {
                return false;
            }
        }
        while pi < pat.len() && pat[pi] == '*' {
            pi += 1;
        }
        pi == pat.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::FsyncPolicy;

    /// Build a single-tenant store and run a command directly through
    /// `handle_command` (no sharding) for deterministic unit testing.
    fn run(store: &mut HashMap<String, TenantState>, tenant: &str, cmd: Command) -> RespValue {
        handle_command(
            store,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000,
            cmd,
            None,
        )
    }

    #[test]
    fn test_string_set_get_memory_accounting() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Set {
                key: "k".to_string(),
                value: b"hello".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            },
        );
        let resp = run(
            &mut store,
            tenant,
            Command::Get {
                key: "k".to_string(),
            },
        );
        assert_eq!(resp, RespValue::BulkString(Some(b"hello".to_vec())));

        let state = store.get(tenant).unwrap();
        assert!(
            state.used_bytes >= 6,
            "used_bytes should account for key+value"
        );
        assert_eq!(state.cache.len(), 1);
    }

    #[test]
    fn test_incr_decr() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let r = run(
            &mut store,
            tenant,
            Command::Incr {
                key: "counter".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(1));

        let r = run(
            &mut store,
            tenant,
            Command::Incrby {
                key: "counter".to_string(),
                delta: 9,
            },
        );
        assert_eq!(r, RespValue::Integer(10));

        let r = run(
            &mut store,
            tenant,
            Command::Decr {
                key: "counter".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(9));
    }

    #[test]
    fn test_append() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Set {
                key: "k".to_string(),
                value: b"foo".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            },
        );
        let r = run(
            &mut store,
            tenant,
            Command::Append {
                key: "k".to_string(),
                value: b"bar".to_vec(),
            },
        );
        assert_eq!(r, RespValue::Integer(6));
        let r = run(
            &mut store,
            tenant,
            Command::Get {
                key: "k".to_string(),
            },
        );
        assert_eq!(r, RespValue::BulkString(Some(b"foobar".to_vec())));
    }

    #[test]
    fn test_type_and_exists() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Sadd {
                key: "s".to_string(),
                member: "m".to_string(),
            },
        );
        let r = run(
            &mut store,
            tenant,
            Command::Type {
                key: "s".to_string(),
            },
        );
        assert_eq!(r, RespValue::SimpleString("set".to_string()));

        let r = run(
            &mut store,
            tenant,
            Command::Exists {
                key: "s".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(1));

        let r = run(
            &mut store,
            tenant,
            Command::Exists {
                key: "missing".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_hash_memory_accounting_on_update() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Hset {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
        );
        let before = store.get(tenant).unwrap().used_bytes;
        let _ = run(
            &mut store,
            tenant,
            Command::Hset {
                key: "h".to_string(),
                field: "f2".to_string(),
                value: b"v2".to_vec(),
            },
        );
        let after = store.get(tenant).unwrap().used_bytes;
        assert!(after > before, "hash update should grow used_bytes");

        let _ = run(
            &mut store,
            tenant,
            Command::Hdel {
                key: "h".to_string(),
                field: "f".to_string(),
            },
        );
        let after_del = store.get(tenant).unwrap().used_bytes;
        assert!(after_del < after, "hdel should shrink used_bytes");
    }

    #[test]
    fn test_rename() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Set {
                key: "a".to_string(),
                value: b"x".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            },
        );
        let r = run(
            &mut store,
            tenant,
            Command::Rename {
                from: "a".to_string(),
                to: "b".to_string(),
            },
        );
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
        let r = run(
            &mut store,
            tenant,
            Command::Get {
                key: "b".to_string(),
            },
        );
        assert_eq!(r, RespValue::BulkString(Some(b"x".to_vec())));
        let r = run(
            &mut store,
            tenant,
            Command::Exists {
                key: "a".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_keys_glob() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        for k in ["user:1", "user:2", "order:1"] {
            let _ = run(
                &mut store,
                tenant,
                Command::Set {
                    key: k.to_string(),
                    value: b"v".to_vec(),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                },
            );
        }
        let r = run(
            &mut store,
            tenant,
            Command::Keys {
                pattern: "user:*".to_string(),
            },
        );
        match r {
            RespValue::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_zrank_and_zrevrange() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Zadd {
                key: "z".to_string(),
                score: 10.0,
                member: "a".to_string(),
            },
        );
        let _ = run(
            &mut store,
            tenant,
            Command::Zadd {
                key: "z".to_string(),
                score: 20.0,
                member: "b".to_string(),
            },
        );
        let r = run(
            &mut store,
            tenant,
            Command::Zrank {
                key: "z".to_string(),
                member: "b".to_string(),
            },
        );
        assert_eq!(r, RespValue::Integer(1));

        let r = run(
            &mut store,
            tenant,
            Command::Zrevrange {
                key: "z".to_string(),
                start: 0,
                stop: -1,
            },
        );
        match r {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sunion_sdiff() {
        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        let _ = run(
            &mut store,
            tenant,
            Command::Sadd {
                key: "s1".to_string(),
                member: "a".to_string(),
            },
        );
        let _ = run(
            &mut store,
            tenant,
            Command::Sadd {
                key: "s1".to_string(),
                member: "b".to_string(),
            },
        );
        let _ = run(
            &mut store,
            tenant,
            Command::Sadd {
                key: "s2".to_string(),
                member: "b".to_string(),
            },
        );
        let _ = run(
            &mut store,
            tenant,
            Command::Sadd {
                key: "s2".to_string(),
                member: "c".to_string(),
            },
        );

        let r = run(
            &mut store,
            tenant,
            Command::Sunion {
                keys: vec!["s1".to_string(), "s2".to_string()],
            },
        );
        match r {
            RespValue::Array(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected array"),
        }

        let r = run(
            &mut store,
            tenant,
            Command::Sdiff {
                keys: vec!["s1".to_string(), "s2".to_string()],
            },
        );
        match r {
            RespValue::Array(items) => assert_eq!(items.len(), 1),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_aof_round_trip() {
        let temp_dir = std::env::temp_dir().join(format!("ultracache_rt_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let aof = AofManager::new(&temp_dir, FsyncPolicy::Always).unwrap();
        let aof_ref = Some(&aof);

        let mut store: HashMap<String, TenantState> = HashMap::new();
        let tenant = "t1";

        // Mutating commands should be logged.
        let _ = handle_command(
            &mut store,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000_000,
            Command::Set {
                key: "k".to_string(),
                value: b"v".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            },
            aof_ref,
        );
        let _ = handle_command(
            &mut store,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000_000,
            Command::Incr {
                key: "c".to_string(),
            },
            aof_ref,
        );
        // Read-only command should NOT be logged.
        let _ = handle_command(
            &mut store,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000_000,
            Command::Get {
                key: "k".to_string(),
            },
            aof_ref,
        );

        // Replay into a fresh store.
        let replayed = aof.replay_commands(tenant).unwrap();
        assert_eq!(replayed.len(), 2, "only mutating commands should be logged");

        let mut store2: HashMap<String, TenantState> = HashMap::new();
        for args in &replayed {
            let cmd = parse_args_to_command(args).expect("parse");
            handle_command(
                &mut store2,
                tenant.to_string(),
                64 * 1024 * 1024,
                5_000_000,
                cmd,
                None,
            );
        }

        let r = handle_command(
            &mut store2,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000_000,
            Command::Get {
                key: "k".to_string(),
            },
            None,
        );
        assert_eq!(r, RespValue::BulkString(Some(b"v".to_vec())));
        let r = handle_command(
            &mut store2,
            tenant.to_string(),
            64 * 1024 * 1024,
            5_000_000,
            Command::Get {
                key: "c".to_string(),
            },
            None,
        );
        assert_eq!(r, RespValue::BulkString(Some(b"1".to_vec())));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_glob_matcher() {
        let m = glob_to_regex("user:*").unwrap();
        assert!(m.is_match("user:123"));
        assert!(!m.is_match("order:123"));

        let m = glob_to_regex("a?c").unwrap();
        assert!(m.is_match("abc"));
        assert!(!m.is_match("ac"));

        let m = glob_to_regex("*").unwrap();
        assert!(m.is_match("anything"));
    }
}
