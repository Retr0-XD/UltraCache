use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// Command-line arguments. All fields are optional and fall back to the config
/// file (then built-in defaults) when omitted.
#[derive(Parser, Debug)]
#[command(name = "UltraCache", version, about = "A fast, multi-tenant, Redis-compatible cache")]
pub struct Cli {
    /// Address to bind the TCP listener to (host:port).
    #[arg(short = 'a', long, env = "ULTRACACHE_ADDR")]
    pub addr: Option<String>,

    /// Number of shards (defaults to number of logical CPUs).
    #[arg(short = 's', long, env = "ULTRACACHE_SHARDS")]
    pub shards: Option<usize>,

    /// Path to a JSON or TOML config file.
    #[arg(short = 'c', long, env = "ULTRACACHE_CONFIG")]
    pub config: Option<PathBuf>,

    /// Enable AOF (append-only file) persistence.
    #[arg(long, env = "ULTRACACHE_AOF")]
    pub aof: bool,

    /// Directory where AOF files are stored.
    #[arg(long, env = "ULTRACACHE_AOF_DIR")]
    pub aof_dir: Option<PathBuf>,

    /// Fsync policy for AOF: always | everysec | no.
    #[arg(long, env = "ULTRACACHE_AOF_FSYNC")]
    pub aof_fsync: Option<String>,

    /// StateLedger base URL (e.g. http://localhost:8080). When set, mutating
    /// cache commands are emitted as verifiable audit records to StateLedger.
    #[arg(long, env = "ULTRACACHE_LEDGER_URL")]
    pub ledger_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AofConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_aof_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_fsync")]
    pub fsync_policy: String,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: default_aof_dir(),
            fsync_policy: default_fsync(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_addr")]
    pub addr: String,
    #[serde(default)]
    pub shards: Option<usize>,
    #[serde(default)]
    pub aof: AofConfig,
    /// StateLedger base URL. Empty when the audit bridge is disabled.
    #[serde(default)]
    pub ledger_endpoint: String,
}

fn default_addr() -> String {
    "0.0.0.0:6379".to_string()
}

fn default_aof_dir() -> PathBuf {
    PathBuf::from("./data/aof")
}

fn default_fsync() -> String {
    "everysec".to_string()
}

impl Config {
    /// Resolve the effective configuration by layering: built-in defaults
    /// < config file < CLI flags.
    pub fn load() -> Config {
        let cli = Cli::parse();
        let mut config = match &cli.config {
            Some(path) => load_from_file(path).unwrap_or_else(|e| {
                eprintln!("warning: could not read config file {path:?}: {e}; using defaults");
                Config::default()
            }),
            None => Config::default(),
        };

        if let Some(addr) = cli.addr {
            config.addr = addr;
        }
        if let Some(shards) = cli.shards {
            config.shards = Some(shards);
        }
        if cli.aof {
            config.aof.enabled = true;
        }
        if let Some(dir) = cli.aof_dir {
            config.aof.dir = dir;
        }
        if let Some(fsync) = cli.aof_fsync {
            config.aof.fsync_policy = fsync;
        }
        if let Some(url) = cli.ledger_url {
            config.ledger_endpoint = url;
        }

        config
    }
}

fn load_from_file(path: &std::path::Path) -> Result<Config, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    let config = if path.extension().and_then(|e| e.to_str()) == Some("toml") {
        toml::from_str(&text)?
    } else {
        serde_json::from_str(&text)?
    };
    Ok(config)
}

impl Default for Config {
    fn default() -> Self {
        Config {
            addr: default_addr(),
            shards: None,
            aof: AofConfig::default(),
            ledger_endpoint: String::new(),
        }
    }
}
