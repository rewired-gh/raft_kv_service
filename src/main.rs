use anyhow::Result;
use raft_kv_service::{config::AppConfig, telemetry};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()?;
    telemetry::init(&config.telemetry)?;
    raft_kv_service::run(config).await
}
