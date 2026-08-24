pub mod config;
pub mod http;
pub mod kv;
pub mod peer;
pub mod raft;
pub mod service;
pub mod storage;
pub mod telemetry;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use config::AppConfig;
use http::AppState;
use raft::NodeId;
use service::KvNodeHandle;
use tokio::net::TcpListener;
use tracing::{info, warn};

pub async fn run(config: AppConfig) -> Result<()> {
    let node = KvNodeHandle::start_one_node(
        NodeId(config.raft.node_id),
        &config.raft.data_file,
        config.raft.request_queue_capacity,
    )
    .context("failed to start the local Raft node")?;
    let address = SocketAddr::new(config.server.host, config.server.port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind HTTP listener to {address}"))?;
    let state = AppState::new(config.service.name).with_executor(move |command| {
        let node = node.clone();
        async move { node.execute(command).await }
    });
    let app = http::router(state.clone(), config.server.request_timeout());

    state.set_ready(true);
    info!(address = %address, "service is ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state))
        .await
        .context("HTTP server terminated unexpectedly")
}

async fn shutdown_signal(state: AppState) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => warn!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }

    state.set_ready(false);
    info!("shutdown requested; draining connections");
}
