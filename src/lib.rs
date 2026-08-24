pub mod cluster;
pub mod config;
pub mod history;
pub mod http;
pub mod kv;
pub mod peer;
pub mod raft;
pub mod service;
pub mod storage;
pub mod telemetry;

use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use config::AppConfig;
use http::AppState;
use peer::ClusterId;
use raft::NodeId;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

pub async fn run(config: AppConfig) -> Result<()> {
    let cluster_uuid =
        uuid::Uuid::parse_str(&config.raft.cluster_id).context("raft.cluster_id must be a UUID")?;
    let endpoints = config
        .raft
        .peer_endpoints
        .iter()
        .map(|peer| (NodeId(peer.node_id), peer.url.clone()))
        .collect::<BTreeMap<_, _>>();
    if endpoints.len() != config.raft.peer_endpoints.len() {
        anyhow::bail!("raft.peer_endpoints contains duplicate node IDs");
    }
    let running = cluster::start_static_node(cluster::StaticNodeConfig {
        cluster_id: ClusterId(*cluster_uuid.as_bytes()),
        node_id: NodeId(config.raft.node_id),
        voters: config.raft.voters.iter().copied().map(NodeId).collect(),
        data_file: config.raft.data_file.clone(),
        request_queue_capacity: config.raft.request_queue_capacity,
        outbound_queue_capacity: config.raft.outbound_queue_capacity,
        election_timeout_min: Duration::from_millis(config.raft.election_timeout_min_ms),
        election_timeout_max: Duration::from_millis(config.raft.election_timeout_max_ms),
        heartbeat_interval: Duration::from_millis(config.raft.heartbeat_interval_ms),
        rpc_timeout: Duration::from_millis(config.raft.rpc_timeout_ms),
        max_clients: config.raft.max_clients,
        snapshot_threshold_entries: config.raft.snapshot_threshold_entries,
        peer_endpoints: endpoints,
    })
    .map_err(anyhow::Error::msg)
    .context("failed to start the static Raft node")?;
    let (node, peer_service) = running.spawn_dispatcher();
    let address = SocketAddr::new(config.server.host, config.server.port);
    let peer_address = SocketAddr::new(config.peer.host, config.peer.port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind HTTP listener to {address}"))?;
    let state = AppState::new(config.service.name).with_executor(move |command| {
        let node = node.clone();
        async move { node.execute(command).await }
    });
    let app = http::router(state.clone(), config.server.request_timeout());

    state.set_ready(true);
    info!(address = %address, peer_address = %peer_address, "service is ready");

    let (shutdown, shutdown_receiver) = watch::channel(false);
    let signal_state = state.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_state.set_ready(false);
        let _ = shutdown.send(true);
    });
    let http = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown_receiver.clone()));
    let peer = cluster::serve_peer(
        peer_address,
        peer_service,
        wait_for_shutdown(shutdown_receiver),
    );
    tokio::try_join!(
        async { http.await.context("HTTP server terminated unexpectedly") },
        async {
            peer.await
                .context("peer RPC server terminated unexpectedly")
        },
    )?;
    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

async fn shutdown_signal() {
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

    info!("shutdown requested; draining connections");
}
