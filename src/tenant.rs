use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct Tenant {
    pub id: String,
    pub memory_limit_bytes: u64,
    pub cpu_quota_micros_per_sec: u64,
}

#[derive(Debug, Clone)]
pub struct TenantRegistry {
    inner: Arc<RwLock<HashMap<String, Tenant>>>,
}

impl TenantRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn resolve_or_create(&self, token: &str) -> Option<Tenant> {
        if token.trim().is_empty() {
            return None;
        }
        let mut guard = self.inner.write().ok()?;
        if let Some(t) = guard.get(token) {
            return Some(t.clone());
        }

        let tenant = Tenant {
            id: token.to_string(),
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            cpu_quota_micros_per_sec: DEFAULT_CPU_QUOTA_MICROS,
        };
        guard.insert(token.to_string(), tenant.clone());
        Some(tenant)
    }
}

const DEFAULT_MEMORY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_CPU_QUOTA_MICROS: u64 = 5_000; // 5ms per second for testing
