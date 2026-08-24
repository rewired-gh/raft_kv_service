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
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RaftConfig {
    pub node_id: u64,
    pub data_file: PathBuf,
    pub request_queue_capacity: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            data_file: PathBuf::from("data/node-1.sqlite3"),
            request_queue_capacity: 256,
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        Config::builder()
            .add_source(File::with_name("config/default").required(false))
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
