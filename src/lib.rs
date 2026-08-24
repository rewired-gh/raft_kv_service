pub mod cluster;
pub mod config;
pub mod history;
pub mod http;
pub mod kv;
pub mod metrics;
pub mod peer;
pub mod raft;
pub mod service;
pub mod storage;
pub mod telemetry;
pub mod tls;

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
    tls::install_crypto_provider();
    if config.server.operational_token.len() < 16 {
        anyhow::bail!("server.operational_token must contain at least 16 bytes");
    }
    let cluster_uuid =
        uuid::Uuid::parse_str(&config.raft.cluster_id).context("raft.cluster_id must be a UUID")?;
    let peer_tls = tls::load_material(&config.peer.tls, true)?;
    let peer_certificates = config
        .raft
        .peer_certificates
        .iter()
        .map(|peer| {
            let bytes = decode_sha256(&peer.sha256).with_context(|| {
                format!("invalid certificate fingerprint for node {}", peer.node_id)
            })?;
            Ok((
                NodeId(peer.node_id),
                cluster::PeerAdmission {
                    certificate_sha256: bytes,
                    staging_expires_unix_seconds: peer.staging_expires_unix_seconds,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let metrics = metrics::Metrics::new();
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
        check_quorum: config.raft.check_quorum,
        leadership_transfer_timeout: Duration::from_millis(
            config.raft.leadership_transfer_timeout_ms,
        ),
        rpc_timeout: Duration::from_millis(config.raft.rpc_timeout_ms),
        max_clients: config.raft.max_clients,
        max_key_bytes: config.raft.max_key_bytes,
        max_value_bytes: config.raft.max_value_bytes,
        snapshot_threshold_entries: config.raft.snapshot_threshold_entries,
        fault_file: config.raft.fault_file.clone(),
        peer_endpoints: endpoints,
        local_peer_endpoint: config.peer.advertised_url.clone(),
        staging: config.raft.staging,
        peer_tls: peer_tls.clone(),
        peer_certificates,
        metrics: metrics.clone(),
    })
    .map_err(anyhow::Error::msg)
    .context("failed to start the static Raft node")?;
    let (node, peer_service) = running.spawn_dispatcher();
    let trace_recorder = config
        .raft
        .trace_file
        .as_deref()
        .map(raft::ProtocolTraceRecorder::create)
        .transpose()
        .context("failed to create protocol trace file")?;
    let address = SocketAddr::new(config.server.host, config.server.port);
    let peer_address = SocketAddr::new(config.peer.host, config.peer.port);
    let operational_address = SocketAddr::new(config.operational.host, config.operational.port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind HTTP listener to {address}"))?;
    let operational_listener = TcpListener::bind(operational_address)
        .await
        .with_context(|| format!("failed to bind operational listener to {operational_address}"))?;
    let client_tls = tls::load_rustls_server(&config.server.tls, false)?;
    let operational_tls = tls::load_rustls_server(&config.operational.tls, true)?;
    let kv_node = node.clone();
    let membership_node = node.clone();
    let membership_reader = node.clone();
    let diagnostics_node = node.clone();
    let transfer_node = node.clone();
    let state = AppState::new(config.service.name)
        .with_command_limits(config.raft.max_key_bytes, config.raft.max_value_bytes)
        .with_metrics(metrics)
        .with_leadership_transfer(move |target| {
            let node = transfer_node.clone();
            async move { node.transfer_leadership(NodeId(target)).await }
        })
        .with_executor(move |command| {
            let node = kv_node.clone();
            async move { node.execute(command).await }
        })
        .with_membership(
            config.server.operational_token.clone(),
            move |operation_id, target| {
                let node = membership_node.clone();
                async move { node.change_membership(operation_id, target).await }
            },
            move || {
                let node = membership_reader.clone();
                async move { node.membership().await }
            },
        )
        .with_diagnostics(move || {
            let node = diagnostics_node.clone();
            async move { node.diagnostics().await }
        });
    let client_app = http::client_router(state.clone(), config.server.request_timeout());
    let operational_app = http::operational_router(state.clone(), config.server.request_timeout());

    state.set_ready(true);
    info!(address = %address, operational_address = %operational_address, peer_address = %peer_address, "service is ready");

    let (shutdown, shutdown_receiver) = watch::channel(false);
    if let Some(recorder) = trace_recorder {
        let trace_node = node.clone();
        let trace_shutdown = shutdown_receiver.clone();
        tokio::spawn(async move {
            record_protocol_state(trace_node, recorder, trace_shutdown).await;
        });
    }
    let signal_state = state.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_state.set_ready(false);
        let _ = shutdown.send(true);
    });
    let http = serve_http(listener, client_app, client_tls, shutdown_receiver.clone());
    let operational = serve_http(
        operational_listener,
        operational_app,
        operational_tls,
        shutdown_receiver.clone(),
    );
    let peer = cluster::serve_peer_tls(
        peer_address,
        peer_service,
        peer_tls,
        wait_for_shutdown(shutdown_receiver),
    );
    tokio::try_join!(
        async { http.await.context("HTTP server terminated unexpectedly") },
        async {
            operational
                .await
                .context("operational HTTP server terminated unexpectedly")
        },
        async {
            peer.await
                .context("peer RPC server terminated unexpectedly")
        },
    )?;
    Ok(())
}

async fn record_protocol_state(
    node: cluster::StaticNodeHandle,
    recorder: raft::ProtocolTraceRecorder,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        match node.diagnostics().await {
            Ok(state) => {
                if let Err(error) = recorder.observe(
                    state.node_id.0,
                    state.term.0,
                    state.last_log_index,
                    state.commit_index,
                    state.applied_index,
                    state.snapshot_index,
                    state.snapshot_term.0,
                ) {
                    warn!(%error, "protocol trace recorder stopped");
                    return;
                }
            }
            Err(error) => {
                warn!(?error, "protocol trace sampler stopped");
                return;
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn serve_http(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<std::sync::Arc<rustls::ServerConfig>>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if let Some(tls) = tls {
        let listener = listener
            .into_std()
            .context("failed to convert TLS listener")?;
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            wait_for_shutdown(shutdown).await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });
        axum_server::from_tcp_rustls(
            listener,
            axum_server::tls_rustls::RustlsConfig::from_config(tls),
        )?
        .handle(handle)
        .serve(app.into_make_service())
        .await
        .context("TLS HTTP server failed")?;
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .context("HTTP server failed")?;
    }
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

fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        anyhow::bail!("SHA-256 fingerprint must contain 64 hexadecimal characters");
    }
    let mut output = [0_u8; 32];
    for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let text = std::str::from_utf8(chunk).context("fingerprint is not UTF-8")?;
        output[index] = u8::from_str_radix(text, 16).context("fingerprint is not hexadecimal")?;
    }
    Ok(output)
}
