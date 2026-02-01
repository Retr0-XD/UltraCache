mod resp;
mod runtime;
mod tenant;

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::resp::{parse_command, RespValue};
use crate::runtime::{Command, RuntimeHandle};
use crate::tenant::TenantRegistry;

const DEFAULT_ADDR: &str = "0.0.0.0:6379";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let registry = Arc::new(TenantRegistry::new());
    let runtime = RuntimeHandle::new(num_cpus::get());

    let _ = registry.resolve_or_create("default");

    let listener = TcpListener::bind(DEFAULT_ADDR).await?;
    println!("UltraCache listening on {DEFAULT_ADDR}");

    loop {
        let (stream, _) = listener.accept().await?;
        let registry = Arc::clone(&registry);
        let runtime = runtime.inner();

        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, registry, runtime).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    registry: Arc<TenantRegistry>,
    runtime: Arc<crate::runtime::ShardRuntime>,
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
) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }

    let command = cmd[0].to_uppercase();
    match command.as_str() {
        "PING" => runtime
            .execute(
                tenant_id.clone(),
                *tenant_limit_bytes,
                *tenant_cpu_quota_micros,
                Command::Ping,
            )
            .await,
        "STATS" => runtime
            .stats(
                tenant_id.clone(),
                *tenant_limit_bytes,
                *tenant_cpu_quota_micros,
            )
            .await,
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
        "SET" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for SET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Set {
                        key: cmd[1].clone(),
                        value: cmd[2].as_bytes().to_vec(),
                    },
                )
                .await
        }
        "DEL" => {
            if cmd.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for DEL".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Del {
                        key: cmd[1].clone(),
                    },
                )
                .await
        }
        "EXPIRE" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for EXPIRE".to_string());
            }
            match cmd[2].parse::<i64>() {
                Ok(seconds) => runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Expire {
                            key: cmd[1].clone(),
                            seconds,
                        },
                    )
                    .await,
                Err(_) => RespValue::Error("ERR value is not an integer or out of range".to_string()),
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
            if cmd.len() < 4 || (cmd.len() - 2) % 2 != 0 {
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
                    return RespValue::Error("ERR value is not an integer or out of range".to_string())
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
            if cmd.len() < 4 || (cmd.len() - 2) % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments for ZADD".to_string());
            }
            let key = cmd[1].clone();
            let mut added: i64 = 0;
            let mut idx = 2;
            while idx < cmd.len() {
                let score = match cmd[idx].parse::<f64>() {
                    Ok(score) => score,
                    Err(_) => return RespValue::Error("ERR value is not a valid float".to_string()),
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
                (Ok(start), Ok(stop)) => runtime
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
                    .await,
                _ => RespValue::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        _ => RespValue::Error("ERR unknown command".to_string()),
    }
}
