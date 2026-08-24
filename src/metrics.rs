//! Bounded-label OpenMetrics instrumentation.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

use crate::{
    peer::PEER_PROTOCOL_VERSION,
    storage::{SNAPSHOT_FORMAT_VERSION, STORAGE_SCHEMA_VERSION},
};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

pub struct RaftMetricsSnapshot {
    pub role: i64,
    pub term: u64,
    pub commit: u64,
    pub applied: u64,
    pub snapshot: u64,
    pub request_queue: usize,
    pub outbound_queue: usize,
}

#[must_use]
pub struct MetricTimer {
    started: Instant,
    histogram: Histogram,
}

impl Drop for MetricTimer {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}

impl fmt::Debug for Metrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Metrics").finish_non_exhaustive()
    }
}

struct MetricsInner {
    registry: Mutex<Registry>,
    pub http_requests: Counter,
    pub rejected_requests: Counter,
    pub forwarded_requests: Counter,
    pub retried_operations: Counter,
    pub timed_out_operations: Counter,
    pub deduplicated_operations: Counter,
    pub raft_elections: Counter,
    pub peer_rpcs: Counter,
    pub snapshots: Counter,
    pub request_latency: Histogram,
    pub election_latency: Histogram,
    pub peer_rpc_latency: Histogram,
    pub replication_latency: Histogram,
    pub apply_latency: Histogram,
    pub snapshot_latency: Histogram,
    pub storage_latency: Histogram,
    pub role: Gauge,
    pub term: Gauge,
    pub commit_index: Gauge,
    pub applied_index: Gauge,
    pub snapshot_index: Gauge,
    pub request_queue_depth: Gauge,
    pub outbound_queue_depth: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let http_requests = Counter::default();
        let rejected_requests = Counter::default();
        let forwarded_requests = Counter::default();
        let retried_operations = Counter::default();
        let timed_out_operations = Counter::default();
        let deduplicated_operations = Counter::default();
        let raft_elections = Counter::default();
        let peer_rpcs = Counter::default();
        let snapshots = Counter::default();
        let latency_buckets =
            || Histogram::new([0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]);
        let request_latency = latency_buckets();
        let election_latency = latency_buckets();
        let peer_rpc_latency = latency_buckets();
        let replication_latency = latency_buckets();
        let apply_latency = latency_buckets();
        let snapshot_latency = latency_buckets();
        let storage_latency = latency_buckets();
        let role = Gauge::default();
        let term = Gauge::default();
        let commit_index = Gauge::default();
        let applied_index = Gauge::default();
        let snapshot_index = Gauge::default();
        let request_queue_depth = Gauge::default();
        let outbound_queue_depth = Gauge::default();
        let mut registry = Registry::default();
        registry.register(
            "http_requests",
            "Accepted HTTP requests.",
            http_requests.clone(),
        );
        registry.register(
            "rejected_requests",
            "Requests rejected before successful execution.",
            rejected_requests.clone(),
        );
        registry.register(
            "forwarded_requests",
            "Client and operational requests forwarded once.",
            forwarded_requests.clone(),
        );
        registry.register(
            "retried_operations",
            "Replication operations retried after rejection.",
            retried_operations.clone(),
        );
        registry.register(
            "timed_out_operations",
            "Bounded operations that timed out.",
            timed_out_operations.clone(),
        );
        registry.register(
            "deduplicated_operations",
            "Client mutations served from durable deduplication state.",
            deduplicated_operations.clone(),
        );
        registry.register(
            "raft_elections",
            "Election timeouts processed.",
            raft_elections.clone(),
        );
        registry.register(
            "peer_rpcs",
            "Validated peer RPCs processed.",
            peer_rpcs.clone(),
        );
        registry.register(
            "snapshots",
            "Snapshots published or installed.",
            snapshots.clone(),
        );
        registry.register(
            "request_duration_seconds",
            "End-to-end HTTP request latency.",
            request_latency.clone(),
        );
        registry.register(
            "election_duration_seconds",
            "Election processing latency.",
            election_latency.clone(),
        );
        registry.register(
            "peer_rpc_duration_seconds",
            "Inbound peer RPC latency.",
            peer_rpc_latency.clone(),
        );
        registry.register(
            "replication_duration_seconds",
            "Outbound replication RPC latency.",
            replication_latency.clone(),
        );
        registry.register(
            "apply_duration_seconds",
            "Committed-entry application latency.",
            apply_latency.clone(),
        );
        registry.register(
            "snapshot_duration_seconds",
            "Snapshot creation or installation latency.",
            snapshot_latency.clone(),
        );
        registry.register(
            "storage_duration_seconds",
            "Storage operation latency observed by the runtime.",
            storage_latency.clone(),
        );
        registry.register(
            "raft_role",
            "Follower=0, candidate=1, leader=2.",
            role.clone(),
        );
        registry.register("raft_term", "Current durable term.", term.clone());
        registry.register(
            "raft_commit_index",
            "Current commit index.",
            commit_index.clone(),
        );
        registry.register(
            "raft_applied_index",
            "Current applied index.",
            applied_index.clone(),
        );
        registry.register(
            "raft_snapshot_index",
            "Current snapshot index.",
            snapshot_index.clone(),
        );
        registry.register(
            "request_queue_depth",
            "Current bounded actor request queue depth.",
            request_queue_depth.clone(),
        );
        registry.register(
            "outbound_queue_depth",
            "Current bounded peer transport queue depth.",
            outbound_queue_depth.clone(),
        );
        Self {
            inner: Arc::new(MetricsInner {
                registry: Mutex::new(registry),
                http_requests,
                rejected_requests,
                forwarded_requests,
                retried_operations,
                timed_out_operations,
                deduplicated_operations,
                raft_elections,
                peer_rpcs,
                snapshots,
                request_latency,
                election_latency,
                peer_rpc_latency,
                replication_latency,
                apply_latency,
                snapshot_latency,
                storage_latency,
                role,
                term,
                commit_index,
                applied_index,
                snapshot_index,
                request_queue_depth,
                outbound_queue_depth,
            }),
        }
    }

    pub fn http_request(&self) -> MetricTimer {
        self.inner.http_requests.inc();
        self.timer(self.inner.request_latency.clone())
    }

    pub fn reject(&self) {
        self.inner.rejected_requests.inc();
    }

    pub fn forwarded(&self) {
        self.inner.forwarded_requests.inc();
    }

    pub fn retried(&self) {
        self.inner.retried_operations.inc();
    }

    pub fn timed_out(&self) {
        self.inner.timed_out_operations.inc();
    }

    pub fn deduplicated(&self) {
        self.inner.deduplicated_operations.inc();
    }

    pub fn election(&self) {
        self.inner.raft_elections.inc();
    }

    pub fn peer_rpc(&self) {
        self.inner.peer_rpcs.inc();
    }

    pub fn snapshot(&self) {
        self.inner.snapshots.inc();
    }

    pub fn time_election(&self) -> MetricTimer {
        self.timer(self.inner.election_latency.clone())
    }

    pub fn time_peer_rpc(&self) -> MetricTimer {
        self.timer(self.inner.peer_rpc_latency.clone())
    }

    pub fn time_replication(&self) -> MetricTimer {
        self.timer(self.inner.replication_latency.clone())
    }

    pub fn time_apply(&self) -> MetricTimer {
        self.timer(self.inner.apply_latency.clone())
    }

    pub fn time_snapshot(&self) -> MetricTimer {
        self.timer(self.inner.snapshot_latency.clone())
    }

    pub fn time_storage(&self) -> MetricTimer {
        self.timer(self.inner.storage_latency.clone())
    }

    fn timer(&self, histogram: Histogram) -> MetricTimer {
        MetricTimer {
            started: Instant::now(),
            histogram,
        }
    }

    pub fn update_raft(&self, snapshot: &RaftMetricsSnapshot) {
        self.inner.role.set(snapshot.role);
        self.inner
            .term
            .set(i64::try_from(snapshot.term).unwrap_or(i64::MAX));
        self.inner
            .commit_index
            .set(i64::try_from(snapshot.commit).unwrap_or(i64::MAX));
        self.inner
            .applied_index
            .set(i64::try_from(snapshot.applied).unwrap_or(i64::MAX));
        self.inner
            .snapshot_index
            .set(i64::try_from(snapshot.snapshot).unwrap_or(i64::MAX));
        self.inner
            .request_queue_depth
            .set(i64::try_from(snapshot.request_queue).unwrap_or(i64::MAX));
        self.inner
            .outbound_queue_depth
            .set(i64::try_from(snapshot.outbound_queue).unwrap_or(i64::MAX));
    }

    pub fn render(&self) -> Result<String, String> {
        let registry = self
            .inner
            .registry
            .lock()
            .map_err(|_| "metrics registry lock is poisoned".to_owned())?;
        let mut output = String::new();
        encode(&mut output, &registry).map_err(|error| error.to_string())?;
        output.push_str(&format!(
            "# TYPE raft_kv_build_info gauge\nraft_kv_build_info{{version=\"{}\",storage_schema=\"{}\",snapshot_format=\"{}\",peer_protocol=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION"),
            STORAGE_SCHEMA_VERSION,
            SNAPSHOT_FORMAT_VERSION,
            PEER_PROTOCOL_VERSION,
        ));
        Ok(output)
    }
}
