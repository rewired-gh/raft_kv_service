use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use raft_kv_service::{
    history::{CheckOptions, Operation, is_linearizable},
    kv::{ClientId, Command, CommandResult, Mutation, MutationCommand, MutationResult, ReadResult},
    raft::ProtocolTrace,
};
use serde::{Deserialize, Serialize};

const CLIENT_COUNT: usize = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_millis(700);

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Campaign {
    schema_version: u16,
    seed: u64,
    requested_operations: usize,
    #[serde(default = "default_cluster_size")]
    cluster_size: usize,
    tool_versions: ToolVersions,
    initial_cluster: Vec<NodeArtifact>,
    final_status: Vec<String>,
    history: Vec<Operation>,
    faults: Vec<FaultEvent>,
    #[serde(default)]
    schedule: Vec<NemesisStep>,
    trace_files: Vec<String>,
    log_files: Vec<String>,
    metrics_files: Vec<String>,
    linearizable: bool,
    minimized_history: Vec<Operation>,
    reproducer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ToolVersions {
    harness: String,
    rustc: String,
    operating_system: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NodeArtifact {
    node_id: u64,
    client_address: String,
    peer_address: String,
    operational_address: String,
    data_file: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FaultEvent {
    elapsed_ms: u128,
    action: String,
    plan: FaultPlan,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct FaultPlan {
    rules: Vec<FaultRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FaultRule {
    from: u64,
    to: u64,
    drop: bool,
    delay_ms: u64,
    duplicates: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NemesisStep {
    at_ms: u64,
    action: NemesisAction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NemesisAction {
    ApplyFault {
        description: String,
        plan: FaultPlan,
    },
    StopNode {
        description: String,
        index: usize,
        plan: FaultPlan,
    },
    StartNode {
        description: String,
        index: usize,
        attempt: usize,
        plan: FaultPlan,
    },
}

struct Arguments {
    seed: u64,
    operations: usize,
    cluster_size: usize,
    artifact_dir: PathBuf,
    replay_schedule: Option<Vec<NemesisStep>>,
}

struct ClusterProcess {
    child: Child,
    node_id: u64,
    trace_path: PathBuf,
}

fn main() -> Result<()> {
    let arguments = arguments()?;
    run(arguments)
}

fn arguments() -> Result<Arguments> {
    let mut seed = 0x6d37_2d72_6166_7421;
    let mut operations = 6;
    let mut cluster_size = default_cluster_size();
    let mut artifact_dir = PathBuf::from("target/fuzz/latest");
    let mut replay = None;
    let mut replay_schedule = None;
    let mut values = std::env::args().skip(1);
    while let Some(argument) = values.next() {
        match argument.as_str() {
            "--seed" => {
                seed = values.next().context("--seed requires a value")?.parse()?;
            }
            "--operations" => {
                operations = values
                    .next()
                    .context("--operations requires a value")?
                    .parse()?;
            }
            "--artifact-dir" => {
                artifact_dir =
                    PathBuf::from(values.next().context("--artifact-dir requires a path")?);
            }
            "--cluster-size" => {
                cluster_size = values
                    .next()
                    .context("--cluster-size requires a value")?
                    .parse()?;
            }
            "--replay" => {
                replay = Some(PathBuf::from(
                    values.next().context("--replay requires a campaign")?,
                ));
            }
            _ => bail!("unknown argument {argument:?}"),
        }
    }
    if let Some(path) = replay {
        let campaign: Campaign = serde_json::from_slice(&std::fs::read(path)?)?;
        if campaign.schema_version != 2 {
            bail!(
                "campaign schema {} cannot provide exact replay; expected schema 2",
                campaign.schema_version
            );
        }
        seed = campaign.seed;
        operations = campaign.requested_operations;
        cluster_size = campaign.cluster_size;
        replay_schedule = Some(campaign.schedule);
    }
    if operations == 0 || operations > 12 {
        bail!("--operations must be between 1 and 12");
    }
    if !matches!(cluster_size, 3 | 5) {
        bail!("--cluster-size must be 3 or 5");
    }
    Ok(Arguments {
        seed,
        operations,
        cluster_size,
        artifact_dir,
        replay_schedule,
    })
}

const fn default_cluster_size() -> usize {
    3
}

fn run(arguments: Arguments) -> Result<()> {
    if arguments.artifact_dir.exists() {
        bail!("artifact directory already exists; choose a new path");
    }
    std::fs::create_dir_all(&arguments.artifact_dir)?;
    let fault_file = arguments.artifact_dir.join("fault-plan.json");
    write_fault_plan(&fault_file, &FaultPlan::default())?;

    let ports = (0..arguments.cluster_size * 3)
        .map(|_| reserve_port())
        .collect::<Result<Vec<_>>>()?;
    let nodes = (0..arguments.cluster_size)
        .map(|index| NodeArtifact {
            node_id: u64::try_from(index + 1).expect("small node ID"),
            client_address: format!("127.0.0.1:{}", ports[index * 3]),
            operational_address: format!("127.0.0.1:{}", ports[index * 3 + 1]),
            peer_address: format!("127.0.0.1:{}", ports[index * 3 + 2]),
            data_file: arguments
                .artifact_dir
                .join(format!("node-{}.sqlite3", index + 1))
                .display()
                .to_string(),
        })
        .collect::<Vec<_>>();
    for node in &nodes {
        write_node_config(&arguments.artifact_dir, &fault_file, node, &nodes)?;
    }

    let service_binary = service_binary()?;
    let processes = Arc::new(Mutex::new(
        (0..arguments.cluster_size)
            .map(|_| None)
            .collect::<Vec<Option<ClusterProcess>>>(),
    ));
    let trace_files = Arc::new(Mutex::new(Vec::<String>::new()));
    let log_files = Arc::new(Mutex::new(Vec::<String>::new()));
    for index in 0..arguments.cluster_size {
        start_node(
            index,
            1,
            &service_binary,
            &arguments.artifact_dir,
            &processes,
            &trace_files,
            &log_files,
        )?;
    }
    let cleanup = ProcessCleanup(processes.clone());
    wait_for_cluster(&nodes, Duration::from_secs(12))?;
    wait_for_leader(&nodes, Duration::from_secs(5))?;
    let started = Instant::now();

    let history = Arc::new(Mutex::new(Vec::<Operation>::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let client_addresses = Arc::new(
        nodes
            .iter()
            .take(2)
            .map(|node| node.client_address.parse::<SocketAddr>())
            .collect::<Result<Vec<_>, _>>()?,
    );

    let mut workers = Vec::new();
    for client in 0..CLIENT_COUNT {
        let history = history.clone();
        let clock = clock.clone();
        let addresses = client_addresses.clone();
        let total = arguments.operations;
        let seed = arguments.seed ^ (u64::try_from(client).expect("client") << 32);
        workers.push(thread::spawn(move || {
            client_workload(client, seed, total, addresses, clock, history)
        }));
    }

    let faults = Arc::new(Mutex::new(Vec::<FaultEvent>::new()));
    let schedule = arguments
        .replay_schedule
        .clone()
        .unwrap_or_else(|| default_nemesis_schedule(arguments.cluster_size));
    validate_schedule(&schedule, arguments.cluster_size)?;
    let nemesis = {
        let schedule = schedule.clone();
        let fault_file = fault_file.clone();
        let processes = processes.clone();
        let service_binary = service_binary.clone();
        let artifact_dir = arguments.artifact_dir.clone();
        let trace_files = trace_files.clone();
        let log_files = log_files.clone();
        let faults = faults.clone();
        thread::spawn(move || -> Result<()> {
            run_nemesis(
                &schedule,
                started,
                &fault_file,
                &service_binary,
                &artifact_dir,
                &processes,
                &trace_files,
                &log_files,
                &faults,
            )
        })
    };

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("client worker panicked"))??;
    }
    nemesis
        .join()
        .map_err(|_| anyhow::anyhow!("nemesis panicked"))??;
    thread::sleep(Duration::from_millis(800));

    let mut history = Arc::try_unwrap(history)
        .map_err(|_| anyhow::anyhow!("history still shared"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("history lock poisoned"))?;
    history.sort_by_key(|operation| operation.id);
    let options = CheckOptions {
        max_operations: 24,
        max_clients: 10_000,
        ..CheckOptions::default()
    };
    let linearizable = is_linearizable(&history, options).context("history check failed")?;
    let minimized_history = if linearizable {
        Vec::new()
    } else {
        minimize(history.clone(), options)?
    };

    let mut final_status = Vec::new();
    let mut metrics_files = Vec::new();
    for node in &nodes {
        final_status.push(
            request(
                node.operational_address.parse()?,
                "GET",
                "/operational/v1/status",
                &[("Authorization", "Bearer local-development-only")],
                &[],
            )
            .map_or_else(
                |error| format!("unavailable: {error:#}"),
                |response| response.raw,
            ),
        );
        let metrics_path = arguments
            .artifact_dir
            .join(format!("node-{}-metrics.txt", node.node_id));
        let metrics = request(
            node.operational_address.parse()?,
            "GET",
            "/metrics",
            &[("Authorization", "Bearer local-development-only")],
            &[],
        )
        .map_or_else(
            |error| format!("unavailable: {error:#}"),
            |response| response.raw,
        );
        std::fs::write(&metrics_path, metrics)?;
        metrics_files.push(relative_name(&metrics_path));
    }

    drop(cleanup);
    let trace_files = trace_files
        .lock()
        .map_err(|_| anyhow::anyhow!("trace file lock poisoned"))?
        .clone();
    for trace in &trace_files {
        let trace = ProtocolTrace::from_json_lines(&arguments.artifact_dir.join(trace))?;
        trace.validate_m4_abstraction()?;
    }
    let campaign = Campaign {
        schema_version: 2,
        seed: arguments.seed,
        requested_operations: arguments.operations,
        cluster_size: arguments.cluster_size,
        tool_versions: ToolVersions {
            harness: env!("CARGO_PKG_VERSION").to_owned(),
            rustc: command_version("rustc", &["--version"]),
            operating_system: std::env::consts::OS.to_owned(),
        },
        initial_cluster: nodes,
        final_status,
        history,
        faults: faults
            .lock()
            .map_err(|_| anyhow::anyhow!("fault lock poisoned"))?
            .clone(),
        schedule,
        trace_files,
        log_files: log_files
            .lock()
            .map_err(|_| anyhow::anyhow!("log file lock poisoned"))?
            .clone(),
        metrics_files,
        linearizable,
        minimized_history,
        reproducer: "./reproduce.sh".to_owned(),
    };
    let campaign_path = arguments.artifact_dir.join("campaign.json");
    std::fs::write(&campaign_path, serde_json::to_vec_pretty(&campaign)?)?;
    write_reproducer(&arguments.artifact_dir)?;
    if !linearizable {
        bail!(
            "linearizability violation; artifacts retained at {}",
            arguments.artifact_dir.display()
        );
    }
    println!("campaign passed: {}", campaign_path.display());
    Ok(())
}

fn client_workload(
    client: usize,
    seed: u64,
    total: usize,
    addresses: Arc<Vec<SocketAddr>>,
    clock: Arc<AtomicU64>,
    history: Arc<Mutex<Vec<Operation>>>,
) -> Result<()> {
    let mut random = fastrand::Rng::with_seed(seed);
    let client_id = ClientId([u8::try_from(client + 1).expect("client ID"); 16]);
    let mut sequence = 0_u64;
    for logical_id in (client..total).step_by(CLIENT_COUNT) {
        let command = generated_command(logical_id, client_id, &mut sequence);
        let mut completed = false;
        for attempt in 0..2_u64 {
            let node = random.usize(0..addresses.len());
            let operation_id = u64::try_from(logical_id)? * 2 + attempt + 1;
            let invoked_at = clock.fetch_add(1, Ordering::SeqCst);
            let observed = invoke(addresses[node], &command);
            let (completed_at, result) = match observed {
                Ok(result) => (Some(clock.fetch_add(1, Ordering::SeqCst)), result),
                Err(_) => (None, None),
            };
            history
                .lock()
                .map_err(|_| anyhow::anyhow!("history lock poisoned"))?
                .push(Operation {
                    id: operation_id,
                    invoked_at,
                    completed_at,
                    command: command.clone(),
                    result: result.clone(),
                });
            if result.is_some() || matches!(command, Command::Read { .. }) {
                completed = true;
                break;
            }
            thread::sleep(Duration::from_millis(40));
        }
        if !completed && matches!(command, Command::Mutate(_)) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn generated_command(logical_id: usize, client_id: ClientId, sequence: &mut u64) -> Command {
    let value = format!("value-{logical_id}").into_bytes();
    if logical_id % 3 == 2 {
        return Command::Read {
            key: "shared".to_owned(),
        };
    }
    *sequence += 1;
    let mutation = match logical_id % 4 {
        0 => Mutation::Create {
            key: "shared".to_owned(),
            value,
        },
        1 | 2 => Mutation::Update {
            key: "shared".to_owned(),
            value,
            expected_revision: None,
        },
        _ => Mutation::Delete {
            key: "shared".to_owned(),
            expected_revision: None,
        },
    };
    Command::Mutate(MutationCommand {
        client_id,
        sequence: *sequence,
        mutation,
    })
}

fn invoke(address: SocketAddr, command: &Command) -> Result<Option<CommandResult>> {
    let mut headers = Vec::<(String, String)>::new();
    let (method, path, body) = match command {
        Command::Read { key } => ("GET", format!("/v1/keys/{key}"), Vec::new()),
        Command::Mutate(mutation) => {
            headers.push((
                "Raft-KV-Client-ID".to_owned(),
                format_uuid(mutation.client_id.0),
            ));
            headers.push((
                "Raft-KV-Request-Sequence".to_owned(),
                mutation.sequence.to_string(),
            ));
            match &mutation.mutation {
                Mutation::Create { key, value } => {
                    headers.push(("If-None-Match".to_owned(), "*".to_owned()));
                    headers.push((
                        "Content-Type".to_owned(),
                        "application/octet-stream".to_owned(),
                    ));
                    ("PUT", format!("/v1/keys/{key}"), value.clone())
                }
                Mutation::Update { key, value, .. } => {
                    headers.push(("If-Match".to_owned(), "*".to_owned()));
                    headers.push((
                        "Content-Type".to_owned(),
                        "application/octet-stream".to_owned(),
                    ));
                    ("PUT", format!("/v1/keys/{key}"), value.clone())
                }
                Mutation::Delete { key, .. } => {
                    headers.push(("If-Match".to_owned(), "*".to_owned()));
                    ("DELETE", format!("/v1/keys/{key}"), Vec::new())
                }
            }
        }
    };
    let header_refs = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let response = request(address, method, &path, &header_refs, &body)?;
    match response.status {
        200 => Ok(Some(CommandResult::Read(ReadResult::Found {
            value: response.body,
            revision: response.revision.context("read response lacks revision")?,
        }))),
        201 => Ok(Some(CommandResult::Mutation(MutationResult::Created {
            revision: response
                .revision
                .context("create response lacks revision")?,
        }))),
        204 => {
            let revision = response
                .revision
                .context("mutation response lacks revision")?;
            let result = match command {
                Command::Mutate(MutationCommand {
                    mutation: Mutation::Delete { .. },
                    ..
                }) => MutationResult::Deleted { revision },
                _ => MutationResult::Updated { revision },
            };
            Ok(Some(CommandResult::Mutation(result)))
        }
        404 if matches!(command, Command::Read { .. }) => {
            Ok(Some(CommandResult::Read(ReadResult::NotFound)))
        }
        412 => Ok(Some(CommandResult::Mutation(
            MutationResult::PreconditionFailed {
                current_revision: response.revision,
            },
        ))),
        429 | 503 | 504 => Ok(None),
        status => bail!("unexpected HTTP status {status}: {}", response.raw),
    }
}

struct HttpResponse {
    status: u16,
    revision: Option<u64>,
    body: Vec<u8>,
    raw: String,
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT)?;
    stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    stream.set_write_timeout(Some(REQUEST_TIMEOUT))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    )?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(body)?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("HTTP response lacks a header terminator")?;
    let headers = std::str::from_utf8(&bytes[..split])?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .context("HTTP response lacks a status")?
        .parse()?;
    let revision = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("raft-kv-revision")
            .then(|| value.trim().parse().ok())
            .flatten()
    });
    Ok(HttpResponse {
        status,
        revision,
        body: bytes[split + 4..].to_vec(),
        raw: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

fn start_node(
    index: usize,
    attempt: usize,
    binary: &Path,
    artifact_dir: &Path,
    processes: &Arc<Mutex<Vec<Option<ClusterProcess>>>>,
    trace_files: &Arc<Mutex<Vec<String>>>,
    log_files: &Arc<Mutex<Vec<String>>>,
) -> Result<()> {
    let trace_name = format!("node-{}-attempt-{attempt}.trace.jsonl", index + 1);
    let log_name = format!("node-{}-attempt-{attempt}.log", index + 1);
    let log = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(artifact_dir.join(&log_name))?;
    let child = ProcessCommand::new(binary)
        .env(
            "RAFT_KV_CONFIG_FILE",
            artifact_dir.join(format!("node-{}", index + 1)),
        )
        .env("RAFT_KV__RAFT__TRACE_FILE", artifact_dir.join(&trace_name))
        .env("RAFT_KV__TELEMETRY__LOG_FORMAT", "json")
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .with_context(|| format!("failed to start node {}", index + 1))?;
    processes
        .lock()
        .map_err(|_| anyhow::anyhow!("process lock poisoned"))?[index] = Some(ClusterProcess {
        child,
        node_id: u64::try_from(index + 1).expect("small node ID"),
        trace_path: artifact_dir.join(&trace_name),
    });
    trace_files
        .lock()
        .map_err(|_| anyhow::anyhow!("trace lock poisoned"))?
        .push(trace_name);
    log_files
        .lock()
        .map_err(|_| anyhow::anyhow!("log lock poisoned"))?
        .push(log_name);
    Ok(())
}

fn stop_node(index: usize, processes: &Arc<Mutex<Vec<Option<ClusterProcess>>>>) -> Result<()> {
    let mut processes = processes
        .lock()
        .map_err(|_| anyhow::anyhow!("process lock poisoned"))?;
    if let Some(mut process) = processes[index].take() {
        process.child.kill()?;
        process.child.wait()?;
        mark_trace_terminated(&process.trace_path, process.node_id)?;
    }
    Ok(())
}

struct ProcessCleanup(Arc<Mutex<Vec<Option<ClusterProcess>>>>);

impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        if let Ok(mut processes) = self.0.lock() {
            for process in processes.iter_mut().filter_map(Option::as_mut) {
                let _ = process.child.kill();
                let _ = process.child.wait();
                let _ = mark_trace_terminated(&process.trace_path, process.node_id);
            }
        }
    }
}

fn mark_trace_terminated(path: &Path, node_id: u64) -> Result<()> {
    let contents = std::fs::read_to_string(path)?;
    let last = contents.lines().last().context("trace file is empty")?;
    let value: serde_json::Value = serde_json::from_str(last)?;
    if value.get("action").and_then(serde_json::Value::as_str) == Some("trace_terminated") {
        return Ok(());
    }
    let sequence = value
        .get("sequence")
        .and_then(serde_json::Value::as_u64)
        .context("trace has no final sequence")?
        .checked_add(1)
        .context("trace sequence exhausted")?;
    let mut output = OpenOptions::new().append(true).open(path)?;
    serde_json::to_writer(
        &mut output,
        &serde_json::json!({
            "sequence": sequence,
            "action": "trace_terminated",
            "node": node_id,
        }),
    )?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn write_node_config(
    artifact_dir: &Path,
    fault_file: &Path,
    node: &NodeArtifact,
    nodes: &[NodeArtifact],
) -> Result<()> {
    let peers = nodes
        .iter()
        .filter(|peer| peer.node_id != node.node_id)
        .map(|peer| {
            format!(
                "{{ node_id = {}, url = \"http://{}\" }}",
                peer.node_id, peer.peer_address
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let client_port = node.client_address.parse::<SocketAddr>()?.port();
    let operational_port = node.operational_address.parse::<SocketAddr>()?.port();
    let peer_port = node.peer_address.parse::<SocketAddr>()?.port();
    let election_min = 100 + node.node_id * 80;
    let election_max = election_min + 30;
    let voters = nodes
        .iter()
        .map(|member| member.node_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let config = format!(
        r#"[server]
host = "127.0.0.1"
port = {client_port}
request_timeout_seconds = 2
operational_token = "local-development-only"

[telemetry]
log_filter = "raft_kv_service=info"
log_format = "json"

[raft]
cluster_id = "00000000-0000-0000-0000-000000000001"
node_id = {node_id}
voters = [{voters}]
peer_endpoints = [{peers}]
data_file = "{data_file}"
fault_file = "{fault_file}"
request_queue_capacity = 256
outbound_queue_capacity = 1024
election_timeout_min_ms = {election_min}
election_timeout_max_ms = {election_max}
heartbeat_interval_ms = 40
rpc_timeout_ms = 120
max_clients = 10000
max_key_bytes = 256
max_value_bytes = 1048576
snapshot_threshold_entries = 4

[peer]
host = "127.0.0.1"
port = {peer_port}
advertised_url = "http://{peer_address}"

[operational]
host = "127.0.0.1"
port = {operational_port}
"#,
        node_id = node.node_id,
        data_file = node.data_file,
        fault_file = fault_file.display(),
        peer_address = node.peer_address,
    );
    std::fs::write(
        artifact_dir.join(format!("node-{}.toml", node.node_id)),
        config,
    )?;
    Ok(())
}

fn default_nemesis_schedule(cluster_size: usize) -> Vec<NemesisStep> {
    let plan = |drop, delay_ms, duplicates| FaultPlan {
        rules: bidirectional_rules(3, cluster_size, drop, delay_ms, duplicates),
    };
    vec![
        NemesisStep {
            at_ms: 40,
            action: NemesisAction::ApplyFault {
                description: "delay and duplicate node-3 traffic".to_owned(),
                plan: plan(false, 60, 1),
            },
        },
        NemesisStep {
            at_ms: 120,
            action: NemesisAction::ApplyFault {
                description: "partition node 3".to_owned(),
                plan: plan(true, 0, 0),
            },
        },
        NemesisStep {
            at_ms: 120,
            action: NemesisAction::StopNode {
                description: "kill node 3 during lag/snapshot window".to_owned(),
                index: 2,
                plan: plan(true, 0, 0),
            },
        },
        NemesisStep {
            at_ms: 320,
            action: NemesisAction::ApplyFault {
                description: "delay node-3 recovery traffic".to_owned(),
                plan: plan(false, 100, 0),
            },
        },
        NemesisStep {
            at_ms: 320,
            action: NemesisAction::StartNode {
                description: "restart node 3 into delayed snapshot recovery".to_owned(),
                index: 2,
                attempt: 2,
                plan: plan(false, 100, 0),
            },
        },
        NemesisStep {
            at_ms: 390,
            action: NemesisAction::StopNode {
                description: "kill node 3 during delayed snapshot transfer".to_owned(),
                index: 2,
                plan: plan(false, 100, 0),
            },
        },
        NemesisStep {
            at_ms: 390,
            action: NemesisAction::ApplyFault {
                description: "heal node-3 recovery traffic".to_owned(),
                plan: FaultPlan::default(),
            },
        },
        NemesisStep {
            at_ms: 390,
            action: NemesisAction::StartNode {
                description: "restart node 3 after interrupted snapshot".to_owned(),
                index: 2,
                attempt: 3,
                plan: FaultPlan::default(),
            },
        },
        NemesisStep {
            at_ms: 570,
            action: NemesisAction::StopNode {
                description: "kill node 1 to force failover".to_owned(),
                index: 0,
                plan: FaultPlan::default(),
            },
        },
        NemesisStep {
            at_ms: 790,
            action: NemesisAction::StartNode {
                description: "restart node 1".to_owned(),
                index: 0,
                attempt: 2,
                plan: FaultPlan::default(),
            },
        },
    ]
}

fn validate_schedule(schedule: &[NemesisStep], cluster_size: usize) -> Result<()> {
    if schedule.is_empty() {
        bail!("nemesis schedule must not be empty");
    }
    let mut previous = 0;
    for step in schedule {
        if step.at_ms < previous || step.at_ms > 60_000 {
            bail!("nemesis schedule timestamps must be ordered and bounded");
        }
        previous = step.at_ms;
        let (node, attempt, plan) = match &step.action {
            NemesisAction::ApplyFault { plan, .. } => (None, None, plan),
            NemesisAction::StopNode { index, plan, .. } => (Some(*index), None, plan),
            NemesisAction::StartNode {
                index,
                attempt,
                plan,
                ..
            } => (Some(*index), Some(*attempt), plan),
        };
        if node.is_some_and(|index| index >= cluster_size) || attempt == Some(0) {
            bail!("nemesis schedule references an invalid node or attempt");
        }
        for rule in &plan.rules {
            let maximum = u64::try_from(cluster_size).expect("bounded cluster size");
            if rule.from == 0
                || rule.to == 0
                || rule.from > maximum
                || rule.to > maximum
                || rule.from == rule.to
                || rule.delay_ms > 60_000
                || rule.duplicates > 2
            {
                bail!("nemesis schedule contains an invalid fault rule");
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_nemesis(
    schedule: &[NemesisStep],
    started: Instant,
    fault_file: &Path,
    service_binary: &Path,
    artifact_dir: &Path,
    processes: &Arc<Mutex<Vec<Option<ClusterProcess>>>>,
    trace_files: &Arc<Mutex<Vec<String>>>,
    log_files: &Arc<Mutex<Vec<String>>>,
    faults: &Arc<Mutex<Vec<FaultEvent>>>,
) -> Result<()> {
    for step in schedule {
        let target = Duration::from_millis(step.at_ms);
        if let Some(remaining) = target.checked_sub(started.elapsed()) {
            thread::sleep(remaining);
        }
        match &step.action {
            NemesisAction::ApplyFault { description, plan } => {
                apply_fault(fault_file, started, description, plan.clone(), faults)?;
            }
            NemesisAction::StopNode {
                description,
                index,
                plan,
            } => {
                stop_node(*index, processes)?;
                record_fault(started, description, plan.clone(), faults);
            }
            NemesisAction::StartNode {
                description,
                index,
                attempt,
                plan,
            } => {
                start_node(
                    *index,
                    *attempt,
                    service_binary,
                    artifact_dir,
                    processes,
                    trace_files,
                    log_files,
                )?;
                record_fault(started, description, plan.clone(), faults);
            }
        }
    }
    Ok(())
}

fn apply_fault(
    path: &Path,
    started: Instant,
    action: &str,
    plan: FaultPlan,
    events: &Arc<Mutex<Vec<FaultEvent>>>,
) -> Result<()> {
    write_fault_plan(path, &plan)?;
    record_fault(started, action, plan, events);
    Ok(())
}

fn record_fault(
    started: Instant,
    action: &str,
    plan: FaultPlan,
    events: &Arc<Mutex<Vec<FaultEvent>>>,
) {
    if let Ok(mut events) = events.lock() {
        events.push(FaultEvent {
            elapsed_ms: started.elapsed().as_millis(),
            action: action.to_owned(),
            plan,
        });
    }
}

fn write_fault_plan(path: &Path, plan: &FaultPlan) -> Result<()> {
    let temporary = path.with_extension("json.next");
    std::fs::write(&temporary, serde_json::to_vec_pretty(plan)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn bidirectional_rules(
    node: u64,
    cluster_size: usize,
    drop: bool,
    delay_ms: u64,
    duplicates: u8,
) -> Vec<FaultRule> {
    (1..=u64::try_from(cluster_size).expect("bounded cluster size"))
        .filter(|peer| *peer != node)
        .flat_map(|peer| [(node, peer), (peer, node)])
        .map(|(from, to)| FaultRule {
            from,
            to,
            drop,
            delay_ms,
            duplicates,
        })
        .collect()
}

fn wait_for_cluster(nodes: &[NodeArtifact], timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if nodes.iter().all(|node| {
            request(
                node.client_address.parse().expect("configured address"),
                "GET",
                "/health/ready",
                &[],
                &[],
            )
            .is_ok_and(|response| response.status == 200)
        }) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    bail!("cluster did not become ready before the deadline")
}

fn wait_for_leader(nodes: &[NodeArtifact], timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if nodes.iter().any(|node| {
            request(
                node.operational_address
                    .parse()
                    .expect("configured address"),
                "GET",
                "/operational/v1/status",
                &[("Authorization", "Bearer local-development-only")],
                &[],
            )
            .is_ok_and(|response| {
                response.status == 200 && response.raw.contains("\"role\":\"leader\"")
            })
        }) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(30));
    }
    bail!("cluster did not elect a leader before the deadline")
}

fn minimize(mut history: Vec<Operation>, options: CheckOptions) -> Result<Vec<Operation>> {
    let mut index = 0;
    while index < history.len() {
        let mut candidate = history.clone();
        candidate.remove(index);
        if !candidate.is_empty() && !is_linearizable(&candidate, options)? {
            history = candidate;
        } else {
            index += 1;
        }
    }
    Ok(history)
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

fn service_binary() -> Result<PathBuf> {
    let sibling = std::env::current_exe()?
        .parent()
        .context("harness executable has no parent")?
        .join("raft-kv-service");
    if !sibling.is_file() {
        bail!(
            "service binary not found at {}; run `cargo build --bins` first",
            sibling.display()
        );
    }
    Ok(sibling)
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let hex = bytes.map(|byte| format!("{byte:02x}")).join("");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn command_version(command: &str, arguments: &[&str]) -> String {
    ProcessCommand::new(command)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || "unavailable".to_owned(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )
}

fn relative_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_owned()
}

fn write_reproducer(artifact_dir: &Path) -> Result<()> {
    let path = artifact_dir.join("reproduce.sh");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(
        b"#!/usr/bin/env bash\nset -euo pipefail\nSCRIPT_DIR=\"$(cd \"$(dirname \"$0\")\" && pwd)\"\ncd \"${SCRIPT_DIR}/../../..\"\ncargo build --locked --bins\ncargo run --locked --bin raft-kv-fuzz -- --replay \"${SCRIPT_DIR}/campaign.json\" --artifact-dir \"${SCRIPT_DIR}/replay\"\n",
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nemesis_schedule_is_canonical_valid_and_round_trips() {
        for cluster_size in [3, 5] {
            let schedule = default_nemesis_schedule(cluster_size);
            validate_schedule(&schedule, cluster_size).expect("default schedule is valid");
            let encoded = serde_json::to_vec(&schedule).expect("encode schedule");
            let decoded: Vec<NemesisStep> =
                serde_json::from_slice(&encoded).expect("decode schedule");
            assert_eq!(decoded.len(), schedule.len());
            assert_eq!(decoded.last().map(|step| step.at_ms), Some(790));
        }
    }

    #[test]
    fn nemesis_schedule_rejects_ambiguous_or_unbounded_inputs() {
        assert!(validate_schedule(&[], 3).is_err());

        let mut reversed = default_nemesis_schedule(3);
        reversed[1].at_ms = 39;
        assert!(validate_schedule(&reversed, 3).is_err());

        let mut too_late = default_nemesis_schedule(3);
        too_late[0].at_ms = 60_001;
        assert!(validate_schedule(&too_late, 3).is_err());

        let mut invalid_node = default_nemesis_schedule(3);
        if let NemesisAction::StopNode { index, .. } = &mut invalid_node[2].action {
            *index = 3;
        }
        assert!(validate_schedule(&invalid_node, 3).is_err());

        let mut invalid_attempt = default_nemesis_schedule(3);
        if let NemesisAction::StartNode { attempt, .. } = &mut invalid_attempt[4].action {
            *attempt = 0;
        }
        assert!(validate_schedule(&invalid_attempt, 3).is_err());

        let mut invalid_rule = default_nemesis_schedule(3);
        if let NemesisAction::ApplyFault { plan, .. } = &mut invalid_rule[0].action {
            plan.rules[0].duplicates = 3;
        }
        assert!(validate_schedule(&invalid_rule, 3).is_err());
    }
}
