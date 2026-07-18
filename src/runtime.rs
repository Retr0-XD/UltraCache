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
    Zrem {
        key: String,
        member: String,
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
        value: Vec<u8>,
    },
    Rpush {
        key: String,
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
            | Command::Del { key }
            | Command::Expire { key, .. }
            | Command::Ttl { key }
            | Command::Hget { key, .. }
            | Command::Hset { key, .. }
            | Command::Hdel { key, .. }
            | Command::Hincrby { key, .. }
            | Command::Hgetall { key }
            | Command::Hkeys { key }
            | Command::Hvals { key }
            | Command::Sadd { key, .. }
            | Command::Srem { key, .. }
            | Command::Smembers { key }
            | Command::Scard { key }
            | Command::Sismember { key, .. }
            | Command::Zadd { key, .. }
            | Command::Zrem { key, .. }
            | Command::Zrange { key, .. }
            | Command::Zcard { key }
            | Command::Zscore { key, .. }
            | Command::Lpush { key, .. }
            | Command::Rpush { key, .. }
            | Command::Lpop { key }
            | Command::Rpop { key }
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
            Command::Flushdb | Command::Ping | Command::Stats => {}
        }
        let hash = hasher.finish() as usize;
        hash % self.shards.len()
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
        Command::Set { ref key, ref value } => {
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
                data: EntryData::String(value.to_vec()),
                expires_at: None,
                size: entry_size,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::SimpleString("OK".to_string())
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
        Command::Lpush { ref key, ref value } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else if let EntryData::List(list) = &mut entry.data {
                    list.push_front(value.clone());
                    let added = (k.len() + value.len()) as u64;
                    entry.size = entry.size.saturating_add(added);
                    state.used_bytes = state.used_bytes.saturating_add(added);
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
            list.push_front(value.clone());
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
            RespValue::Integer(1)
        }
        Command::Rpush { ref key, ref value } => {
            let k = tenant_key(&tenant_id, key);

            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.remove(&k);
                } else if let EntryData::List(list) = &mut entry.data {
                    list.push_back(value.clone());
                    let added = (k.len() + value.len()) as u64;
                    entry.size = entry.size.saturating_add(added);
                    state.used_bytes = state.used_bytes.saturating_add(added);
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
            list.push_back(value.clone());
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
            RespValue::Integer(1)
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
            | Command::Del { .. }
            | Command::Expire { .. }
            | Command::Hset { .. }
            | Command::Hdel { .. }
            | Command::Hincrby { .. }
            | Command::Sadd { .. }
            | Command::Srem { .. }
            | Command::Zadd { .. }
            | Command::Zrem { .. }
            | Command::Lpush { .. }
            | Command::Rpush { .. }
            | Command::Lpop { .. }
            | Command::Rpop { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Incrby { .. }
            | Command::Decrby { .. }
            | Command::Append { .. }
            | Command::Persist { .. }
            | Command::Mset { .. }
            | Command::Flushdb
            | Command::Rename { .. }
    )
}

/// Serialize a command back into RESP bulk-string arguments for AOF logging.
fn command_to_args(cmd: &Command) -> Option<Vec<String>> {
    match cmd {
        Command::Set { key, value } => Some(vec![
            "SET".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Del { key } => Some(vec!["DEL".to_string(), key.clone()]),
        Command::Expire { key, seconds } => {
            Some(vec!["EXPIRE".to_string(), key.clone(), seconds.to_string()])
        }
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
        Command::Sadd { key, member } => {
            Some(vec!["SADD".to_string(), key.clone(), member.clone()])
        }
        Command::Srem { key, member } => {
            Some(vec!["SREM".to_string(), key.clone(), member.clone()])
        }
        Command::Zadd { key, score, member } => Some(vec![
            "ZADD".to_string(),
            key.clone(),
            score.to_string(),
            member.clone(),
        ]),
        Command::Zrem { key, member } => {
            Some(vec!["ZREM".to_string(), key.clone(), member.clone()])
        }
        Command::Lpush { key, value } => Some(vec![
            "LPUSH".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Rpush { key, value } => Some(vec![
            "RPUSH".to_string(),
            key.clone(),
            String::from_utf8_lossy(value).into_owned(),
        ]),
        Command::Lpop { key } => Some(vec!["LPOP".to_string(), key.clone()]),
        Command::Rpop { key } => Some(vec!["RPOP".to_string(), key.clone()]),
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
        "SET" if args.len() == 3 => Command::Set {
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
        "SADD" if args.len() == 3 => Command::Sadd {
            key: args[1].clone(),
            member: args[2].clone(),
        },
        "SREM" if args.len() == 3 => Command::Srem {
            key: args[1].clone(),
            member: args[2].clone(),
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
        "LPUSH" if args.len() == 3 => Command::Lpush {
            key: args[1].clone(),
            value: args[2].as_bytes().to_vec(),
        },
        "RPUSH" if args.len() == 3 => Command::Rpush {
            key: args[1].clone(),
            value: args[2].as_bytes().to_vec(),
        },
        "LPOP" if args.len() == 2 => Command::Lpop {
            key: args[1].clone(),
        },
        "RPOP" if args.len() == 2 => Command::Rpop {
            key: args[1].clone(),
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
