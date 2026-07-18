//! Integration bridge between UltraCache and StateLedger.
//!
//! UltraCache is a fast, multi-tenant data plane. StateLedger is a durable,
//! verifiable state plane (append-only, hash-chained, Merkle-provable). This
//! bridge lets UltraCache publish an immutable, verifiable audit trail of its
//! mutating operations to StateLedger over HTTP. The result is a "verifiable
//! cache": every SET/DEL/INCR/etc. can later be proven to have happened, in
//! order, by querying StateLedger and verifying its hash chain + Merkle root.
//!
//! The bridge is best-effort and non-blocking: a failed emission never blocks
//! or fails the cache operation. It is also optional — when no endpoint is
//! configured, `LedgerBridge::none()` is a no-op.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

/// A single audit event emitted to StateLedger.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub tenant: String,
    pub command: String,
    pub key: String,
    pub summary: String,
}

impl AuditEvent {
    /// Serialize the event as a StateLedger record payload (JSON).
    pub fn payload(&self) -> String {
        // Compact JSON without external deps.
        format!(
            "{{\"tenant\":{},\"command\":{},\"key\":{},\"summary\":{}}}",
            json_str(&self.tenant),
            json_str(&self.command),
            json_str(&self.key),
            json_str(&self.summary),
        )
    }
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Configuration for the StateLedger bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Base URL of the StateLedger REST API, e.g. "http://127.0.0.1:8080".
    pub endpoint: String,
    /// Record type used for emitted audit events.
    pub record_type: String,
    /// Source identifier for emitted audit events.
    pub source: String,
    /// Maximum number of events buffered before dropping.
    #[allow(dead_code)]
    pub buffer: usize,
    /// HTTP request timeout.
    pub timeout: Duration,
}

impl BridgeConfig {
    /// Build a config from environment / CLI-shaped values. Returns None when
    /// the endpoint is empty (bridge disabled).
    pub fn from_endpoint(endpoint: &str) -> Option<BridgeConfig> {
        if endpoint.trim().is_empty() {
            return None;
        }
        Some(BridgeConfig {
            endpoint: endpoint.trim().trim_end_matches('/').to_string(),
            record_type: "cache.audit".to_string(),
            source: "ultracache".to_string(),
            buffer: 1024,
            timeout: Duration::from_secs(2),
        })
    }
}

/// `LedgerBridge` emits audit events to StateLedger asynchronously.
///
/// Cloning the bridge shares the same background worker and channel, so it is
/// cheap to pass a clone into every connection task.
#[derive(Clone)]
pub struct LedgerBridge {
    inner: Option<Arc<BridgeInner>>,
}

struct BridgeInner {
    tx: mpsc::UnboundedSender<AuditEvent>,
    // Keep the worker task alive for the lifetime of the bridge.
    _worker: tokio::task::JoinHandle<()>,
}

impl LedgerBridge {
    /// A disabled bridge that drops all events.
    pub fn none() -> Self {
        LedgerBridge { inner: None }
    }

    /// Whether the bridge is enabled (has a configured endpoint).
    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Create an enabled bridge and spawn its background emission worker.
    pub fn new(config: BridgeConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<AuditEvent>();
        let worker = tokio::spawn(bridge_worker(config, rx));
        let inner = BridgeInner { tx, _worker: worker };
        LedgerBridge {
            inner: Some(Arc::new(inner)),
        }
    }

    /// Emit an audit event. Returns false if the bridge is disabled or the
    /// buffer is full (the event is dropped, never blocks the caller).
    pub fn emit(&self, event: AuditEvent) -> bool {
        match &self.inner {
            Some(inner) => inner.tx.send(event).is_ok(),
            None => false,
        }
    }
}

async fn bridge_worker(config: BridgeConfig, mut rx: mpsc::UnboundedReceiver<AuditEvent>) {
    // A single reusable HTTP client posts audit events to StateLedger's
    // REST API. The `reqwest` crate (rustls TLS backend) keeps the dependency
    // footprint small and avoids OpenSSL on Windows.
    let client = reqwest::Client::builder()
        .timeout(config.timeout)
        .build();

    let client = match client {
        Ok(c) => c,
        Err(e) => {
            eprintln!("bridge: failed to build http client: {e}");
            return;
        }
    };

    let url = format!("{}/api/v1/records", config.endpoint);

    while let Some(event) = rx.recv().await {
        let body = format!(
            "{{\"type\":{},\"source\":{},\"payload\":{}}}",
            json_str(&config.record_type),
            json_str(&config.source),
            json_str(&event.payload()),
        );

        let res = client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await;

        if let Err(e) = res {
            eprintln!("bridge: failed to emit audit event: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_endpoint_empty_is_none() {
        assert!(BridgeConfig::from_endpoint("").is_none());
        assert!(BridgeConfig::from_endpoint("   ").is_none());
    }

    #[test]
    fn test_from_endpoint_strips_trailing_slash() {
        let cfg = BridgeConfig::from_endpoint("http://localhost:8080/").unwrap();
        assert_eq!(cfg.endpoint, "http://localhost:8080");
        assert_eq!(cfg.record_type, "cache.audit");
        assert_eq!(cfg.source, "ultracache");
    }

    #[test]
    fn test_audit_event_payload_json() {
        let event = AuditEvent {
            tenant: "acme".to_string(),
            command: "SET".to_string(),
            key: "user:1".to_string(),
            summary: "SET user:1 hello".to_string(),
        };
        let payload = event.payload();
        assert!(payload.contains("\"tenant\":\"acme\""));
        assert!(payload.contains("\"command\":\"SET\""));
        assert!(payload.contains("\"key\":\"user:1\""));
        assert!(payload.contains("\"summary\":\"SET user:1 hello\""));
    }

    #[test]
    fn test_audit_event_payload_escapes_quotes() {
        let event = AuditEvent {
            tenant: "t".to_string(),
            command: "SET".to_string(),
            key: "k".to_string(),
            summary: "SET k \"quoted\"".to_string(),
        };
        let payload = event.payload();
        // The inner quote must be escaped.
        assert!(payload.contains("\\\"quoted\\\""));
    }

    #[test]
    fn test_none_bridge_disabled_and_drops() {
        let bridge = LedgerBridge::none();
        assert!(!bridge.enabled());
        let event = AuditEvent {
            tenant: "t".to_string(),
            command: "SET".to_string(),
            key: "k".to_string(),
            summary: "SET k v".to_string(),
        };
        assert!(!bridge.emit(event));
    }
}
