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
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for HSET".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Hset {
                        key: cmd[1].clone(),
                        field: cmd[2].clone(),
                        value: cmd[3].as_bytes().to_vec(),
                    },
                )
                .await
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
        "SADD" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for SADD".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Sadd {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
        }
        "SREM" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for SREM".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Srem {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
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
        "ZADD" => {
            if cmd.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for ZADD".to_string());
            }
            match cmd[2].parse::<f64>() {
                Ok(score) => runtime
                    .execute(
                        tenant_id.clone(),
                        *tenant_limit_bytes,
                        *tenant_cpu_quota_micros,
                        Command::Zadd {
                            key: cmd[1].clone(),
                            score,
                            member: cmd[3].clone(),
                        },
                    )
                    .await,
                Err(_) => RespValue::Error("ERR value is not a valid float".to_string()),
            }
        }
        "ZREM" => {
            if cmd.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for ZREM".to_string());
            }
            runtime
                .execute(
                    tenant_id.clone(),
                    *tenant_limit_bytes,
                    *tenant_cpu_quota_micros,
                    Command::Zrem {
                        key: cmd[1].clone(),
                        member: cmd[2].clone(),
                    },
                )
                .await
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
