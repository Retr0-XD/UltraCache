use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// AOF (Append-Only File) persistence manager
/// Provides crash recovery and durability guarantees per tenant
#[derive(Debug, Clone)]
pub struct AofManager {
    inner: Arc<RwLock<AofManagerInner>>,
}

#[derive(Debug)]
struct AofManagerInner {
    base_dir: PathBuf,
    files: HashMap<String, BufWriter<File>>,
    fsync_policy: FsyncPolicy,
    last_fsync: HashMap<String, SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Fsync after every write (safest, slowest)
    Always,
    /// Fsync every second (balanced)
    EverySecond,
    /// Never fsync explicitly (fastest, least safe)
    No,
}

impl AofManager {
    pub fn new(base_dir: impl AsRef<Path>, policy: FsyncPolicy) -> std::io::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(AofManagerInner {
                base_dir,
                files: HashMap::new(),
                fsync_policy: policy,
                last_fsync: HashMap::new(),
            })),
        })
    }

    /// Log a command to the AOF for a specific tenant
    pub fn log_command(&self, tenant_id: &str, command: &[String]) -> std::io::Result<()> {
        let mut inner = self.inner.write().unwrap();

        // Get or create AOF file for this tenant
        if !inner.files.contains_key(tenant_id) {
            let aof_path = inner.base_dir.join(format!("{}.aof", tenant_id));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(aof_path)?;
            inner
                .files
                .insert(tenant_id.to_string(), BufWriter::new(file));
            inner
                .last_fsync
                .insert(tenant_id.to_string(), SystemTime::now());
        }

        let fsync_policy = inner.fsync_policy;
        let last_fsync_time = inner.last_fsync.get(tenant_id).copied();
        let writer = inner.files.get_mut(tenant_id).unwrap();

        // Write in RESP array format for easy parsing
        writeln!(writer, "*{}", command.len())?;
        for arg in command {
            writeln!(writer, "${}", arg.len())?;
            writeln!(writer, "{}", arg)?;
        }

        // Apply fsync policy
        match fsync_policy {
            FsyncPolicy::Always => {
                writer.flush()?;
                writer.get_ref().sync_all()?;
            }
            FsyncPolicy::EverySecond => {
                let now = SystemTime::now();
                if let Some(last) = last_fsync_time
                    && now.duration_since(last).unwrap().as_secs() >= 1
                {
                    writer.flush()?;
                    writer.get_ref().sync_all()?;
                    inner.last_fsync.insert(tenant_id.to_string(), now);
                }
            }
            FsyncPolicy::No => {
                // Let OS handle flushing
            }
        }

        Ok(())
    }

    /// Replay AOF file for a tenant to recover state
    pub fn replay_commands(&self, tenant_id: &str) -> std::io::Result<Vec<Vec<String>>> {
        let inner = self.inner.read().unwrap();
        let aof_path = inner.base_dir.join(format!("{}.aof", tenant_id));

        if !aof_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(aof_path)?;
        let reader = BufReader::new(file);
        let mut commands = Vec::new();
        let mut lines = reader.lines();

        while let Some(Ok(line)) = lines.next() {
            // Parse RESP array format
            if !line.starts_with('*') {
                continue;
            }

            let argc = line[1..].parse::<usize>().unwrap_or(0);
            let mut command = Vec::new();

            for _ in 0..argc {
                // Read bulk string length
                if let Some(Ok(bulk_line)) = lines.next() {
                    if !bulk_line.starts_with('$') {
                        break;
                    }
                    // Read actual value
                    if let Some(Ok(value)) = lines.next() {
                        command.push(value);
                    }
                }
            }

            if command.len() == argc && argc > 0 {
                commands.push(command);
            }
        }

        Ok(commands)
    }

    /// Rewrite AOF file to compact it (remove redundant commands)
    #[allow(dead_code)]
    pub fn rewrite_aof(
        &self,
        tenant_id: &str,
        snapshot_commands: Vec<Vec<String>>,
    ) -> std::io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        let aof_path = inner.base_dir.join(format!("{}.aof", tenant_id));
        let temp_path = inner.base_dir.join(format!("{}.aof.tmp", tenant_id));

        // Write compacted snapshot to temp file
        let temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;
        let mut writer = BufWriter::new(temp_file);

        for command in snapshot_commands {
            writeln!(writer, "*{}", command.len())?;
            for arg in command {
                writeln!(writer, "${}", arg.len())?;
                writeln!(writer, "{}", arg)?;
            }
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);

        // Atomic rename
        std::fs::rename(temp_path, &aof_path)?;

        // Reopen the file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(aof_path)?;
        inner
            .files
            .insert(tenant_id.to_string(), BufWriter::new(file));

        Ok(())
    }

    /// Flush all buffers and sync to disk
    #[allow(dead_code)]
    pub fn flush_all(&self) -> std::io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        for writer in inner.files.values_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Close AOF file for a tenant
    #[allow(dead_code)]
    pub fn close_tenant(&self, tenant_id: &str) -> std::io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        if let Some(mut writer) = inner.files.remove(tenant_id) {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        inner.last_fsync.remove(tenant_id);
        Ok(())
    }

    /// List tenant IDs that have an existing AOF file on disk. Used during
    /// startup recovery to discover which tenants need replaying.
    pub fn list_tenants(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut tenants = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&inner.base_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("aof")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    tenants.push(stem.to_string());
                }
            }
        }
        tenants
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_basic_logging() {
        let temp_dir = std::env::temp_dir().join(format!("ultracache_test_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let aof = AofManager::new(&temp_dir, FsyncPolicy::Always).unwrap();

        let commands = vec![
            vec!["SET".to_string(), "key1".to_string(), "value1".to_string()],
            vec!["SET".to_string(), "key2".to_string(), "value2".to_string()],
        ];

        for cmd in &commands {
            aof.log_command("tenant1", cmd).unwrap();
        }

        let replayed = aof.replay_commands("tenant1").unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0], commands[0]);
        assert_eq!(replayed[1], commands[1]);

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_aof_rewrite() {
        let temp_dir =
            std::env::temp_dir().join(format!("ultracache_test_{}", std::process::id() + 1));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let aof = AofManager::new(&temp_dir, FsyncPolicy::Always).unwrap();

        // Log many redundant commands
        for i in 0..10 {
            aof.log_command(
                "tenant1",
                &["SET".to_string(), "counter".to_string(), i.to_string()],
            )
            .unwrap();
        }

        // Rewrite to just the final state
        let snapshot = vec![vec![
            "SET".to_string(),
            "counter".to_string(),
            "9".to_string(),
        ]];
        aof.rewrite_aof("tenant1", snapshot.clone()).unwrap();

        let replayed = aof.replay_commands("tenant1").unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], snapshot[0]);

        std::fs::remove_dir_all(&temp_dir).ok();
    }
}
