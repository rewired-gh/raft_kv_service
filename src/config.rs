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
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerEndpointConfig {
    pub node_id: u64,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct PeerServerConfig {
    pub host: IpAddr,
    pub port: u16,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 9080,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8080,
            request_timeout_seconds: 30,
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
