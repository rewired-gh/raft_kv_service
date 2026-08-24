use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub service: ServiceConfig,
    pub server: ServerConfig,
    pub telemetry: TelemetryConfig,
    pub raft: RaftConfig,
    pub peer: PeerServerConfig,
    pub operational: OperationalServerConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RaftConfig {
    pub cluster_id: String,
    pub node_id: u64,
    pub voters: Vec<u64>,
    pub peer_endpoints: Vec<PeerEndpointConfig>,
    pub data_file: PathBuf,
    pub request_queue_capacity: usize,
    pub outbound_queue_capacity: usize,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub rpc_timeout_ms: u64,
    pub max_clients: u64,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub snapshot_threshold_entries: u64,
    pub trace_file: Option<PathBuf>,
    pub fault_file: Option<PathBuf>,
    pub staging: bool,
    pub peer_certificates: Vec<PeerCertificateConfig>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            cluster_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            node_id: 1,
            voters: vec![1],
            peer_endpoints: Vec::new(),
            data_file: PathBuf::from("data/node-1.sqlite3"),
            request_queue_capacity: 256,
            outbound_queue_capacity: 1024,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            heartbeat_interval_ms: 100,
            rpc_timeout_ms: 250,
            max_clients: 10_000,
            max_key_bytes: crate::kv::MAX_KEY_BYTES,
            max_value_bytes: crate::kv::MAX_VALUE_BYTES,
            snapshot_threshold_entries: 10_000,
            trace_file: None,
            fault_file: None,
            staging: false,
            peer_certificates: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerEndpointConfig {
    pub node_id: u64,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerCertificateConfig {
    pub node_id: u64,
    pub sha256: String,
    pub staging_expires_unix_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct PeerServerConfig {
    pub host: IpAddr,
    pub port: u16,
    pub advertised_url: String,
    pub tls: TlsFileConfig,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 9080,
            advertised_url: "http://127.0.0.1:9080".to_owned(),
            tls: TlsFileConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct TlsFileConfig {
    pub certificate_file: Option<PathBuf>,
    pub private_key_file: Option<PathBuf>,
    pub client_ca_file: Option<PathBuf>,
}

impl TlsFileConfig {
    pub fn enabled(&self) -> bool {
        self.certificate_file.is_some()
            || self.private_key_file.is_some()
            || self.client_ca_file.is_some()
    }

    pub fn validate(&self, require_client_ca: bool) -> Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        if self.certificate_file.is_none()
            || self.private_key_file.is_none()
            || (require_client_ca && self.client_ca_file.is_none())
        {
            anyhow::bail!(
                "TLS requires certificate_file and private_key_file{}",
                if require_client_ca {
                    " and client_ca_file"
                } else {
                    ""
                }
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct OperationalServerConfig {
    pub host: IpAddr,
    pub port: u16,
    pub tls: TlsFileConfig,
}

impl Default for OperationalServerConfig {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 8088,
            tls: TlsFileConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let mut builder =
            Config::builder().add_source(File::with_name("config/default").required(false));
        if let Ok(path) = std::env::var("RAFT_KV_CONFIG_FILE") {
            builder = builder.add_source(File::with_name(&path).required(true));
        }
        builder
            .add_source(
                Environment::with_prefix("RAFT_KV")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .context("failed to build application configuration")?
            .try_deserialize()
            .context("failed to deserialize application configuration")
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServiceConfig {
    pub name: String,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME").to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: IpAddr,
    pub port: u16,
    pub request_timeout_seconds: u64,
    pub operational_token: String,
    pub tls: TlsFileConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8080,
            request_timeout_seconds: 30,
            operational_token: "local-development-only".to_owned(),
            tls: TlsFileConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    pub log_filter: String,
    pub log_format: LogFormat,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_filter: format!("{}=debug,tower_http=info", env!("CARGO_CRATE_NAME")),
            log_format: LogFormat::Pretty,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    #[default]
    Pretty,
}
