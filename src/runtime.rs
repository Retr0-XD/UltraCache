use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use tokio::sync::{mpsc, oneshot};

use crate::resp::RespValue;

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Del { key: String },
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
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

    fn route_shard(&self, tenant_id: &str, command: &Command) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tenant_id.hash(&mut hasher);
        match command {
            Command::Get { key }
            | Command::Set { key, .. }
            | Command::Del { key }
            | Command::Expire { key, .. }
            | Command::Ttl { key } => {
                key.hash(&mut hasher)
            }
            Command::Ping => {}
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
        Command::Get { key } => {
            let k = tenant_key(&tenant_id, &key);
            match state.cache.get(&k) {
                Some(entry) => {
                    if TenantState::is_expired(entry) {
                        state.cache.pop(&k);
                        RespValue::BulkString(None)
                    } else {
                        RespValue::BulkString(Some(entry.data.clone()))
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
                state.used_bytes = state.used_bytes.saturating_sub(entry_size_bytes(&k, &existing.data));
            }

            let mut projected = state.used_bytes + entry_size;
            while projected > state.limit_bytes {
                if let Some((ek, ev)) = state.cache.pop_lru() {
                    state.used_bytes =
                        state.used_bytes.saturating_sub(entry_size_bytes(&ek, &ev.data));
                    projected = state.used_bytes + entry_size;
                } else {
                    return RespValue::Error("ERR tenant memory limit exceeded".to_string());
                }
            }

            let entry = Entry {
                data: value,
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
                state.used_bytes = state.used_bytes.saturating_sub(entry_size_bytes(&k, &v.data));
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
    };
    
    // Record CPU time used
    let elapsed_micros = start.elapsed().as_micros() as u64;
    state.record_cpu_time(elapsed_micros);
    
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

struct Entry {
    data: Vec<u8>,
    expires_at: Option<u64>,
}

struct TenantState {
    cache: LruCache<String, Entry>,
    used_bytes: u64,
    limit_bytes: u64,
    cpu_used_micros: u64,
    cpu_quota_micros: u64,
    last_reset: std::time::Instant,
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

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
