mod bridge;
mod config;
mod persistence;
mod resp;
mod runtime;
mod tenant;

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::persistence::{AofManager, FsyncPolicy};
use crate::resp::{RespValue, parse_command};
use crate::runtime::{Command, RuntimeHandle};
use crate::tenant::TenantRegistry;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = Config::load();

    let registry = Arc::new(TenantRegistry::new());
    let runtime = if config.aof.enabled {
        let policy = match config.aof.fsync_policy.as_str() {
            "always" => FsyncPolicy::Always,
            "everysec" => FsyncPolicy::EverySecond,
            _ => FsyncPolicy::No,
        };
        let aof =
            Arc::new(AofManager::new(&config.aof.dir, policy).map_err(std::io::Error::other)?);
        let handle = RuntimeHandle::with_persistence(num_cpus::get(), Some(aof.clone()));
        if let Err(e) = handle.recover().await {
            eprintln!("warning: AOF recovery failed: {e}");
        } else {
            println!("AOF recovery complete");
        }
        handle
    } else {
        RuntimeHandle::new(num_cpus::get())
    };

    let _ = registry.resolve_or_create("default");

    // Optional verifiable audit bridge to StateLedger.
    let bridge = match bridge::BridgeConfig::from_endpoint(&config.ledger_endpoint) {
        Some(cfg) => {
            println!("StateLedger audit bridge enabled: {}", cfg.endpoint);
            bridge::LedgerBridge::new(cfg)
        }
        None => bridge::LedgerBridge::none(),
    };

    let listener = TcpListener::bind(&config.addr).await?;
    println!("UltraCache listening on {}", config.addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let registry = Arc::clone(&registry);
        let runtime = runtime.inner();
        let bridge = bridge.clone();

        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, registry, runtime, bridge).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    registry: Arc<TenantRegistry>,
    runtime: Arc<crate::runtime::ShardRuntime>,
    bridge: bridge::LedgerBridge,
) -> Result<(), String> {
    let mut tenant_id = "default".to_string();
    let mut tenant_limit_bytes = 64 * 1024 * 1024u64;
    let mut tenant_cpu_quota_micros = 5_000u64;
    let mut buffer: Vec<u8> = Vec::with_capacity(4096);
    let mut temp = [0u8; 4096];

    loop {
        let n = stream
            .read(&mut temp)
            .await
            .map_err(|e| format!("read error: {e}"))?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&temp[..n]);

        loop {
            match parse_command(&buffer) {
                Ok(Some((cmd, consumed))) => {
                    buffer.drain(0..consumed);
                    let response = handle_command(
                        &cmd,
                        &registry,
                        &runtime,
                        &mut tenant_id,
                        &mut tenant_limit_bytes,
                        &mut tenant_cpu_quota_micros,
                        &bridge,
                    )
                    .await;
                    stream
                        .write_all(&response.encode())
                        .await
                        .map_err(|e| format!("write error: {e}"))?;
                }
                Ok(None) => break,
                Err(err) => {
                    let msg = format!("ERR {err:?}");
                    let _ = stream.write_all(&RespValue::Error(msg).encode()).await;
                    return Err("parse error".to_string());
                }
            }
        }
    }
}

async fn handle_command(
    cmd: &[String],
    registry: &TenantRegistry,
    runtime: &crate::runtime::ShardRuntime,
    tenant_id: &mut String,
    tenant_limit_bytes: &mut u64,
    tenant_cpu_quota_micros: &mut u64,
    bridge: &bridge::LedgerBridge,
) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }

    let command = cmd[0].to_uppercase();
    let response = match command.as_str() {
        "PING" => {
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Ping,
                )
                .await
        }
        "STATS" => {
            runtime
                .stats(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                )
                .await
        }
        "AUTH" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for AUTH".to_string());
            }
            match registry.resolve_or_create(&cmd[1]) {
                Some(t) => {
                    *tenant_id = t.id;
                    *tenant_limit_bytes = t.memory_limit_bytes;
                    *tenant_cpu_quota_micros = t.cpu_quota_micros_per_sec;
                    RespValue::SimpleString("OK".to_string())
                }
                None => RespValue::Error("ERR invalid token".to_string()),
            }
        }
        "TENANTS" => {
            if cmd.len() != 1 {
                return RespValue::Error("ERR wrong number of arguments for TENANTS".to_string());
            }
            let tenants = registry.list();
            let mut entries: Vec<RespValue> = tenants
                .into_iter()
                .map(|tenant| {
                    let entry = format!(
                        "id={} memory_limit_bytes={} cpu_quota_micros={}",
                        tenant.id, tenant.memory_limit_bytes, tenant.cpu_quota_micros_per_sec
                    );
                    RespValue::BulkString(Some(entry.into_bytes()))
                })
                .collect();
            entries.sort_by(|a, b| match (a, b) {
                (RespValue::BulkString(Some(left)), RespValue::BulkString(Some(right))) => {
                    left.cmp(right)
                }
                _ => std::cmp::Ordering::Equal,
            });
            RespValue::Array(entries)
        }
        "GET" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for GET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Get {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "GETSET" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for GETSET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::GetSet {
                        key: cmd[1].clone(),
                        value: cmd[2].as_bytes().to_vec(),
                    },
                )
                .await
        }
        "STRLEN" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for STRLEN".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::StrLen {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "SET" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for SET".to_string());
            }
            let mut ex: Option<i64> = None;
            let mut px: Option<i64> = None;
            let mut nx = false;
            let mut xx = false;
            let mut i = 3;
            while i < cmd.len() {
                match cmd[i].to_uppercase().as_str() {
                    "EX" if i + 1 < cmd.len() => {
                        match cmd[i + 1].parse::<i64>() {
                            Ok(v) => ex = Some(v),
                            Err(_) => {
                                return RespValue::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                );
                            }
                        }
                        i += 2;
                    }
                    "PX" if i + 1 < cmd.len() => {
                        match cmd[i + 1].parse::<i64>() {
                            Ok(v) => px = Some(v),
                            Err(_) => {
                                return RespValue::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                );
                            }
                        }
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
                    _ => return RespValue::Error("ERR syntax error in SET options".to_string()),
                }
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Set {
                        key: cmd[1].clone(),
                        value: cmd[2].as_bytes().to_vec(),
                        ex,
                        px,
                        nx,
                        xx,
                    },
                )
                .await
        }
        "DEL" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for DEL".to_string());
            }
            let mut removed: i64 = 0;
            for key in cmd[1..].iter() {
                match runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Del { key: key.clone() },
                    )
                    .await
                {
                    RespValue::Integer(n) => removed += n,
                    RespValue::Error(_) => continue,
                    _ => {}
                }
            }
            RespValue::Integer(removed)
        }
        "EXPIRE" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for EXPIRE".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(seconds) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Expire {
                                key: cmd[1].clone(),
                                seconds,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "PEXPIRE" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for PEXPIRE".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(milliseconds) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Pexpire {
                                key: cmd[1].clone(),
                                milliseconds,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "PEXPIREAT" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for PEXPIREAT".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(timestamp) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Pexpireat {
                                key: cmd[1].clone(),
                                milliseconds_timestamp: timestamp,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "EXPIREAT" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for EXPIREAT".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(timestamp) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Expireat {
                                key: cmd[1].clone(),
                                timestamp,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "TTL" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for TTL".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Ttl {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "HGET" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for HGET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hget {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                    },
                )
                .await
        }
        "HSET" => {
            if cmd.len() < 4 || !(cmd.len() - 2).is_multiple_of(2) {
                return RespValue::Error("ERR wrong number of arguments for HSET".to_string());
            }
            let key = cmd[1].clone();
            let mut added: i64 = 0;
            let mut idx = 2;
            while idx < cmd.len() {
                let field = cmd[idx].clone();
                let value = cmd[idx + 1].as_bytes().to_vec();
                let resp = runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Hset {
                            key: key.clone(),
                            field,
                            value,
                        },
                    )
                    .await;
                match resp {
                    RespValue::Integer(n) => added += n,
                    RespValue::Error(_) => return resp,
                    _ => return RespValue::Error("ERR invalid response".to_string()),
                }
                idx += 2;
            }
            RespValue::Integer(added)
        }
        "HDEL" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for HDEL".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hdel {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                    },
                )
                .await
        }
        "HINCRBY" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for HINCRBY".to_string());
            }
            let delta = match cmd[3].parse::<i64>() {
                Ok(delta) => delta,
                Err(_) => {
                    return RespValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    );
                }
            };
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hincrby {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                        delta,
                    },
                )
                .await
        }
        "HINCRBYFLOAT" => {
            if cmd.len() != 4 {
                return RespValue::Error(
                    "ERR wrong number of arguments for HINCRBYFLOAT".to_string(),
                );
            }
            let delta = match cmd[3].parse::<f64>() {
                Ok(delta) => delta,
                Err(_) => {
                    return RespValue::Error(
                        "ERR value is not a float or out of range".to_string(),
                    );
                }
            };
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hincrbyfloat {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                        delta,
                    },
                )
                .await
        }
        "HSETNX" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for HSETNX".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hsetnx {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                        value: cmd[3].as_bytes().to_vec(),
                    },
                )
                .await
        }
        "SADD" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for SADD".to_string());
            }
            let key = cmd[1].clone();
            let mut added: i64 = 0;
            for member in cmd[2..].iter() {
                let resp = runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Sadd {
                            key: key.clone(),
                            member: member.clone(),
                        },
                    )
                    .await;
                match resp {
                    RespValue::Integer(n) => added += n,
                    RespValue::Error(_) => return resp,
                    _ => return RespValue::Error("ERR invalid response".to_string()),
                }
            }
            RespValue::Integer(added)
        }
        "SREM" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for SREM".to_string());
            }
            let key = cmd[1].clone();
            let mut removed: i64 = 0;
            for member in cmd[2..].iter() {
                let resp = runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Srem {
                            key: key.clone(),
                            member: member.clone(),
                        },
                    )
                    .await;
                match resp {
                    RespValue::Integer(n) => removed += n,
                    RespValue::Error(_) => return resp,
                    _ => return RespValue::Error("ERR invalid response".to_string()),
                }
            }
            RespValue::Integer(removed)
        }
        "SMEMBERS" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for SMEMBERS".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Smembers {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "SMOVE" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for SMOVE".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Smove {
                        source: cmd[1].clone(),
                        destination: cmd[2].clone(),
                        member: cmd[3].clone(),
                    },
                )
                .await
        }
        "SPOP" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for SPOP".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Spop {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "SINTER" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for SINTER".to_string());
            }
            runtime
                .sinter(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    cmd[1..].to_vec(),
                )
                .await
        }
        "ZADD" => {
            if cmd.len() < 4 || !(cmd.len() - 2).is_multiple_of(2) {
                return RespValue::Error("ERR wrong number of arguments for ZADD".to_string());
            }
            let key = cmd[1].clone();
            let mut added: i64 = 0;
            let mut idx = 2;
            while idx < cmd.len() {
                let score = match cmd[idx].parse::<f64>() {
                    Ok(score) => score,
                    Err(_) => {
                        return RespValue::Error("ERR value is not a valid float".to_string());
                    }
                };
                let member = cmd[idx + 1].clone();
                let resp = runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Zadd {
                            key: key.clone(),
                            score,
                            member,
                        },
                    )
                    .await;
                match resp {
                    RespValue::Integer(n) => added += n,
                    RespValue::Error(_) => return resp,
                    _ => return RespValue::Error("ERR invalid response".to_string()),
                }
                idx += 2;
            }
            RespValue::Integer(added)
        }
        "ZREM" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for ZREM".to_string());
            }
            let key = cmd[1].clone();
            let mut removed: i64 = 0;
            for member in cmd[2..].iter() {
                let resp = runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Zrem {
                            key: key.clone(),
                            member: member.clone(),
                        },
                    )
                    .await;
                match resp {
                    RespValue::Integer(n) => removed += n,
                    RespValue::Error(_) => return resp,
                    _ => return RespValue::Error("ERR invalid response".to_string()),
                }
            }
            RespValue::Integer(removed)
        }
        "ZRANGE" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for ZRANGE".to_string());
            }
            match (cmd[2].parse::<i64>(), cmd[3].parse::<i64>()) {
                (Ok(start), Ok(stop)) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Zrange {
                                key: cmd[1].clone(),
                                start,
                                stop,
                            },
                        )
                        .await
                }
                _ => RespValue::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "ZCARD" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for ZCARD".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zcard {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "ZSCORE" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for ZSCORE".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zscore {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
        }
        "ZINCRBY" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for ZINCRBY".to_string());
            }
            let delta = match cmd[2].parse::<f64>() {
                Ok(delta) => delta,
                Err(_) => {
                    return RespValue::Error("ERR value is not a valid float".to_string());
                }
            };
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zincrby {
                        key: cmd[1].clone(),
                        delta,
                        member: cmd[3].clone(),
                    },
                )
                .await
        }
        "ZRANGEBYSCORE" => {
            if cmd.len() != 4 {
                return RespValue::Error(
                    "ERR wrong number of arguments for ZRANGEBYSCORE".to_string(),
                );
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zrangebyscore {
                        key: cmd[1].clone(),
                        min: cmd[2].clone(),
                        max: cmd[3].clone(),
                    },
                )
                .await
        }
        "ZREMRANGEBYSCORE" => {
            if cmd.len() != 4 {
                return RespValue::Error(
                    "ERR wrong number of arguments for ZREMRANGEBYSCORE".to_string(),
                );
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zremrangebyscore {
                        key: cmd[1].clone(),
                        min: cmd[2].clone(),
                        max: cmd[3].clone(),
                    },
                )
                .await
        }
        "ZCOUNT" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for ZCOUNT".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zcount {
                        key: cmd[1].clone(),
                        min: cmd[2].clone(),
                        max: cmd[3].clone(),
                    },
                )
                .await
        }
        "HGETALL" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HGETALL".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hgetall {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "HKEYS" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HKEYS".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hkeys {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "HVALS" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HVALS".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hvals {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "SCARD" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for SCARD".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Scard {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "SISMEMBER" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for SISMEMBER".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Sismember {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
        }
        "LPUSH" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for LPUSH".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Lpush {
                        key: cmd[1].clone(),
                        values: cmd[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
                    },
                )
                .await
        }
        "LPUSHX" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for LPUSHX".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Lpushx {
                        key: cmd[1].clone(),
                        values: cmd[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
                    },
                )
                .await
        }
        "RPUSH" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for RPUSH".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Rpush {
                        key: cmd[1].clone(),
                        values: cmd[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
                    },
                )
                .await
        }
        "RPUSHX" => {
            if cmd.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for RPUSHX".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Rpushx {
                        key: cmd[1].clone(),
                        values: cmd[2..].iter().map(|a| a.as_bytes().to_vec()).collect(),
                    },
                )
                .await
        }
        "LPOP" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for LPOP".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Lpop {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "RPOP" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for RPOP".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Rpop {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "LLEN" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for LLEN".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Llen {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "LRANGE" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for LRANGE".to_string());
            }
            match (cmd[2].parse::<i64>(), cmd[3].parse::<i64>()) {
                (Ok(start), Ok(stop)) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Lrange {
                                key: cmd[1].clone(),
                                start,
                                stop,
                            },
                        )
                        .await
                }
                _ => RespValue::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "LINDEX" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for LINDEX".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(index) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Lindex {
                                key: cmd[1].clone(),
                                index,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "LSET" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for LSET".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(index) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Lset {
                                key: cmd[1].clone(),
                                index,
                                value: cmd[3].as_bytes().to_vec(),
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "LTRIM" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for LTRIM".to_string());
            }
            match (cmd[2].parse::<i64>(), cmd[3].parse::<i64>()) {
                (Ok(start), Ok(stop)) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Ltrim {
                                key: cmd[1].clone(),
                                start,
                                stop,
                            },
                        )
                        .await
                }
                _ => RespValue::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "LINSERT" => {
            if cmd.len() != 5 {
                return RespValue::Error("ERR wrong number of arguments for LINSERT".to_string());
            }
            let before = match cmd[2].to_uppercase().as_str() {
                "BEFORE" => true,
                "AFTER" => false,
                _ => {
                    return RespValue::Error(
                        "ERR syntax error: LINSERT must be BEFORE or AFTER".to_string(),
                    );
                }
            };
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Linsert {
                        key: cmd[1].clone(),
                        before,
                        pivot: cmd[3].as_bytes().to_vec(),
                        value: cmd[4].as_bytes().to_vec(),
                    },
                )
                .await
        }
        "INCR" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for INCR".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Incr {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "DECR" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for DECR".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Decr {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "INCRBY" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for INCRBY".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(delta) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Incrby {
                                key: cmd[1].clone(),
                                delta,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "DECRBY" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for DECRBY".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(delta) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Decrby {
                                key: cmd[1].clone(),
                                delta,
                            },
                        )
                        .await
                }
                Err(_) => {
                    RespValue::Error("ERR value is not an integer or out of range".to_string())
                }
            }
        }
        "APPEND" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for APPEND".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Append {
                        key: cmd[1].clone(),
                        value: cmd[2].as_bytes().to_vec(),
                    },
                )
                .await
        }
        "EXISTS" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for EXISTS".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Exists {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "TYPE" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for TYPE".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Type {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "PERSIST" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for PERSIST".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Persist {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "PTTL" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for PTTL".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Pttl {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "MSET" => {
            if cmd.len() < 3 || !(cmd.len() - 1).is_multiple_of(2) {
                return RespValue::Error("ERR wrong number of arguments for MSET".to_string());
            }
            let mut pairs = Vec::with_capacity((cmd.len() - 1) / 2);
            let mut idx = 1;
            while idx + 1 < cmd.len() {
                pairs.push((cmd[idx].clone(), cmd[idx + 1].as_bytes().to_vec()));
                idx += 2;
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Mset { pairs },
                )
                .await
        }
        "MGET" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for MGET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Mget {
                        keys: cmd[1..].to_vec(),
                    },
                )
                .await
        }
        "KEYS" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for KEYS".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Keys {
                        pattern: cmd[1].clone(),
                    },
                )
                .await
        }
        "FLUSHDB" => {
            if cmd.len() != 1 {
                return RespValue::Error("ERR wrong number of arguments for FLUSHDB".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Flushdb,
                )
                .await
        }
        "RENAME" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for RENAME".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Rename {
                        from: cmd[1].clone(),
                        to: cmd[2].clone(),
                    },
                )
                .await
        }
        "ZRANK" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for ZRANK".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zrank {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
        }
        "ZREVRANGE" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for ZREVRANGE".to_string());
            }
            match (cmd[2].parse::<i64>(), cmd[3].parse::<i64>()) {
                (Ok(start), Ok(stop)) => {
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::Zrevrange {
                                key: cmd[1].clone(),
                                start,
                                stop,
                            },
                        )
                        .await
                }
                _ => RespValue::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "SUNION" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for SUNION".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Sunion {
                        keys: cmd[1..].to_vec(),
                    },
                )
                .await
        }
        "SDIFF" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for SDIFF".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Sdiff {
                        keys: cmd[1..].to_vec(),
                    },
                )
                .await
        }
        "DBSIZE" => {
            if cmd.len() != 1 {
                return RespValue::Error("ERR wrong number of arguments for DBSIZE".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Dbsize,
                )
                .await
        }
        "RANDOMKEY" => {
            if cmd.len() != 1 {
                return RespValue::Error("ERR wrong number of arguments for RANDOMKEY".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Randomkey,
                )
                .await
        }
        "ECHO" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for ECHO".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Echo {
                        message: cmd[1].clone(),
                    },
                )
                .await
        }
        "INFO" => {
            if cmd.len() != 1 {
                return RespValue::Error("ERR wrong number of arguments for INFO".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Info,
                )
                .await
        }
        "CONFIG" => {
            if cmd.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for CONFIG".to_string());
            }
            match cmd[1].to_uppercase().as_str() {
                "GET" => {
                    if cmd.len() != 3 {
                        return RespValue::Error(
                            "ERR wrong number of arguments for CONFIG GET".to_string(),
                        );
                    }
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::ConfigGet {
                                parameter: cmd[2].clone(),
                            },
                        )
                        .await
                }
                "SET" => {
                    if cmd.len() != 4 {
                        return RespValue::Error(
                            "ERR wrong number of arguments for CONFIG SET".to_string(),
                        );
                    }
                    runtime
                        .execute(
                            tenant_id.clone(),
                            *tenant_limit_bytes,
                            *tenant_cpu_quota_micros,
                            Command::ConfigSet {
                                parameter: cmd[2].clone(),
                                value: cmd[3].clone(),
                            },
                        )
                        .await
                }
                _ => RespValue::Error("ERR CONFIG subcommand must be GET or SET".to_string()),
            }
        }
        _ => RespValue::Error("ERR unknown command".to_string()),
    };

    // Emit a verifiable audit event to StateLedger for mutating commands.
    if bridge.enabled()
        && is_audit_command(&command)
        && !matches!(response, RespValue::Error(_))
        && let Some(event) = audit_event(&command, cmd, tenant_id)
    {
        bridge.emit(event);
    }

    response
}

/// Returns true for commands that mutate tenant state and should be audited.
fn is_audit_command(command: &str) -> bool {
    matches!(
        command,
        "SET"
            | "DEL"
            | "EXPIRE"
            | "PEXPIRE"
            | "PEXPIREAT"
            | "EXPIREAT"
            | "HSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HSETNX"
            | "SADD"
            | "SREM"
            | "SMOVE"
            | "SPOP"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZREMRANGEBYSCORE"
            | "LPUSH"
            | "RPUSH"
            | "LPUSHX"
            | "RPUSHX"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "LTRIM"
            | "LINSERT"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "APPEND"
            | "GETSET"
            | "PERSIST"
            | "MSET"
            | "FLUSHDB"
            | "RENAME"
            | "CONFIG"
    )
}

/// Build an `AuditEvent` for a mutating command. Returns None for commands
/// whose arguments are malformed (already rejected earlier, so this is just a
/// safety net).
fn audit_event(command: &str, cmd: &[String], tenant_id: &str) -> Option<bridge::AuditEvent> {
    let key = match command {
        "MSET" => format!("{} keys", (cmd.len() - 1) / 2),
        "MGET" => format!("{} keys", cmd.len() - 1),
        "FLUSHDB" => "*".to_string(),
        "RENAME" if cmd.len() >= 3 => format!("{} -> {}", cmd[1], cmd[2]),
        "SUNION" | "SDIFF" => cmd[1..].join(","),
        _ if cmd.len() >= 2 => cmd[1].clone(),
        _ => return None,
    };

    let summary = cmd.join(" ");
    Some(bridge::AuditEvent {
        tenant: tenant_id.to_string(),
        command: command.to_string(),
        key,
        summary,
    })
}
