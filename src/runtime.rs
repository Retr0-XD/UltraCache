use std::collections::{HashMap, HashSet, BTreeMap};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use tokio::sync::{mpsc, oneshot};

use crate::resp::RespValue;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Stats,  // Admin command for tenant stats
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Del { key: String },
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
    // Hash commands
    Hget { key: String, field: String },
    Hset { key: String, field: String, value: Vec<u8> },
    Hdel { key: String, field: String },
    Hincrby { key: String, field: String, delta: i64 },
    // Set commands
    Sadd { key: String, member: String },
    Srem { key: String, member: String },
    Smembers { key: String },
    // Sorted Set commands
    Zadd { key: String, score: f64, member: String },
    Zrem { key: String, member: String },
    Zrange { key: String, start: i64, stop: i64 },
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
}

impl ShardRuntime {
    pub fn new(num_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (tx, mut rx) = mpsc::channel::<ShardRequest>(1024);
            shards.push(tx);

            tokio::spawn(async move {
                let mut store: HashMap<String, TenantState> = HashMap::new();
                while let Some(req) = rx.recv().await {
                    let response = handle_command(
                        &mut store,
                        req.tenant_id,
                        req.tenant_limit_bytes,
                        req.cpu_quota_micros,
                        req.command,
                    );
                    let _ = req.respond_to.send(response);
                }
            });
        }

        Self { shards }
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

            if let Err(_) = tx.send(req).await {
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

        if let Err(_) = self.shards[shard_idx].send(req).await {
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
                            RespValue::BulkString(Some(bytes)) => {
                                String::from_utf8(bytes).ok()
                            }
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

                    if intersection.as_ref().map_or(true, |s| s.is_empty()) {
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
            | Command::Sadd { key, .. }
            | Command::Srem { key, .. }
            | Command::Smembers { key }
            | Command::Zadd { key, .. }
            | Command::Zrem { key, .. }
            | Command::Zrange { key, .. } => {
                key.hash(&mut hasher)
            }
            Command::Ping | Command::Stats => {}
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
        Command::Get { key } => {
            let k = tenant_key(&tenant_id, &key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.cache.pop(&k);
                        RespValue::BulkString(None)
                    } else {
                        match &entry.data {
                            EntryData::String(bytes) => RespValue::BulkString(Some(bytes.clone())),
                            _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                        }
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Set { key, value } => {
            let k = tenant_key(&tenant_id, &key);
            let entry_size = entry_size_bytes(&k, &value);
            if entry_size > state.limit_bytes {
                return RespValue::Error("ERR tenant memory limit exceeded".to_string());
            }

            if let Some(existing) = state.cache.pop(&k) {
                state.used_bytes = state.used_bytes.saturating_sub(calculate_entry_size(&k, &existing.data));
            }

            let mut projected = state.used_bytes + entry_size;
            while projected > state.limit_bytes {
                if let Some((ek, ev)) = state.cache.pop_lru() {
                    state.used_bytes =
                        state.used_bytes.saturating_sub(calculate_entry_size(&ek, &ev.data));
                    state.record_eviction();
                    projected = state.used_bytes + entry_size;
                } else {
                    return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                }
            }

            let entry = Entry {
                data: EntryData::String(value),
                expires_at: None,
            };
            state.cache.put(k, entry);
            state.used_bytes = state.used_bytes.saturating_add(entry_size);
            RespValue::SimpleString("OK".to_string())
        }
        Command::Del { key } => {
            let k = tenant_key(&tenant_id, &key);
            let removed = state.cache.pop(&k);
            if let Some(v) = removed {
                state.used_bytes = state.used_bytes.saturating_sub(calculate_entry_size(&k, &v.data));
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Expire { key, seconds } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                    return RespValue::Integer(0);
                }
                if seconds <= 0 {
                    state.cache.pop(&k);
                    return RespValue::Integer(1);
                }
                entry.expires_at = Some(current_timestamp() + seconds as u64);
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Ttl { key } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
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
        Command::Hget { key, field } => {
            let k = tenant_key(&tenant_id, &key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.cache.pop(&k);
                        return RespValue::BulkString(None);
                    }
                    match &entry.data {
                        EntryData::Hash(map) => {
                            match map.get(&field) {
                                Some(val) => RespValue::BulkString(Some(val.clone())),
                                None => RespValue::BulkString(None),
                            }
                        }
                        _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }
        Command::Hset { key, field, value } => {
            let k = tenant_key(&tenant_id, &key);
            
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            let is_new_field = map.insert(field.clone(), value.clone()).is_none();
                            return RespValue::Integer(if is_new_field { 1 } else { 0 });
                        }
                        _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
            }
            
            // Create new hash
            let mut map = HashMap::new();
            map.insert(field, value);
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
            };
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Hdel { key, field } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::Hash(map) => {
                        let removed = map.remove(&field).is_some();
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Hincrby { key, field, delta } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Hash(map) => {
                            let current = match map.get(&field) {
                                Some(val) => match std::str::from_utf8(val)
                                    .ok()
                                    .and_then(|s| s.parse::<i64>().ok())
                                {
                                    Some(num) => num,
                                    None => {
                                        return RespValue::Error(
                                            "ERR hash value is not an integer".to_string(),
                                        )
                                    }
                                },
                                None => 0,
                            };
                            let next = current.saturating_add(delta);
                            map.insert(field, next.to_string().into_bytes());
                            return RespValue::Integer(next);
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            )
                        }
                    }
                }
            }

            let mut map = HashMap::new();
            let next = delta;
            map.insert(field, next.to_string().into_bytes());
            let entry = Entry {
                data: EntryData::Hash(map),
                expires_at: None,
            };
            state.cache.put(k, entry);
            RespValue::Integer(next)
        }
        // Set commands
        Command::Sadd { key, member } => {
            let k = tenant_key(&tenant_id, &key);
            
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                } else {
                    match &mut entry.data {
                        EntryData::Set(set) => {
                            let added = set.insert(member.clone());
                            return RespValue::Integer(if added { 1 } else { 0 });
                        }
                        _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
            }
            
            // Create new set
            let mut set = HashSet::new();
            set.insert(member);
            let entry = Entry {
                data: EntryData::Set(set),
                expires_at: None,
            };
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Srem { key, member } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::Set(set) => {
                        let removed = set.remove(&member);
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Smembers { key } => {
            let k = tenant_key(&tenant_id, &key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.cache.pop(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::Set(set) => {
                            let members: Vec<RespValue> = set.iter()
                                .map(|m| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(members)
                        }
                        _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
        // Sorted Set commands
        Command::Zadd { key, score, member } => {
            let k = tenant_key(&tenant_id, &key);
            
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                } else {
                    match &mut entry.data {
                        EntryData::ZSet(zset) => {
                            let is_new = zset.insert(member.clone(), score).is_none();
                            return RespValue::Integer(if is_new { 1 } else { 0 });
                        }
                        _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
            }
            
            // Create new zset
            let mut zset = BTreeMap::new();
            zset.insert(member, score);
            let entry = Entry {
                data: EntryData::ZSet(zset),
                expires_at: None,
            };
            state.cache.put(k, entry);
            RespValue::Integer(1)
        }
        Command::Zrem { key, member } => {
            let k = tenant_key(&tenant_id, &key);
            if let Some(entry) = state.cache.get_mut(&k) {
                if TenantState::is_expired(entry) {
                    state.cache.pop(&k);
                    return RespValue::Integer(0);
                }
                match &mut entry.data {
                    EntryData::ZSet(zset) => {
                        let removed = zset.remove(&member).is_some();
                        RespValue::Integer(if removed { 1 } else { 0 })
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            } else {
                RespValue::Integer(0)
            }
        }
        Command::Zrange { key, start, stop } => {
            let k = tenant_key(&tenant_id, &key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.cache.pop(&k);
                        return RespValue::Array(vec![]);
                    }
                    match &entry.data {
                        EntryData::ZSet(zset) => {
                            // Collect and sort by score
                            let mut members: Vec<(&String, &f64)> = zset.iter().collect();
                            members.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal));
                            
                            let len = members.len() as i64;
                            let start_idx = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                            let stop_idx = if stop < 0 { (len + stop + 1).max(0) } else { (stop + 1).min(len) } as usize;
                            
                            let result: Vec<RespValue> = members[start_idx..stop_idx.min(members.len())]
                                .iter()
                                .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(result)
                        }
                        _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    }
                }
                None => RespValue::Array(vec![]),
            }
        }
    };
    
    // Record CPU time and latency
    let elapsed_micros = start.elapsed().as_micros() as u64;
    state.record_cpu_time(elapsed_micros);
    state.record_latency(elapsed_micros);
    
    result
}

fn tenant_key(tenant_id: &str, key: &str) -> String {
    format!("{}:{}", tenant_id, key)
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
}

#[derive(Clone)]
enum EntryData {
    String(Vec<u8>),
    Hash(std::collections::HashMap<String, Vec<u8>>),
    Set(std::collections::HashSet<String>),
    ZSet(std::collections::BTreeMap<String, f64>),
}

struct TenantState {
    cache: LruCache<String, Entry>,
    used_bytes: u64,
    limit_bytes: u64,
    cpu_used_micros: u64,
    cpu_quota_micros: u64,
    last_reset: std::time::Instant,
    // Latency tracking (simple histogram)
    latency_samples: Vec<u64>,  // Store last N latencies in microseconds
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
        EntryData::Hash(map) => {
            map.iter()
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum::<u64>()
        }
        EntryData::Set(set) => {
            set.iter().map(|s| s.len() as u64).sum::<u64>()
        }
        EntryData::ZSet(map) => {
            map.iter()
                .map(|(member, _score)| member.len() as u64 + 8)
                .sum::<u64>()
        }
    };
    key_size + data_size
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
