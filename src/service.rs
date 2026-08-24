//! Bounded service-to-Raft command bridge.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    sync::{Arc, Mutex, mpsc as std_mpsc},
    thread,
};

use tokio::sync::{mpsc, oneshot};

use crate::{
    http::ServiceError,
    kv::{Command, CommandResult, encode_command},
    raft::{DurableRaft, Effect, EntryPayload, Event, NodeId, VoterSet},
    storage::SqliteStorage,
};

#[derive(Clone)]
pub struct KvNodeHandle {
    inner: Arc<KvNodeInner>,
}

struct KvNodeInner {
    requests: Mutex<Option<mpsc::Sender<Request>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Drop for KvNodeInner {
    fn drop(&mut self) {
        if let Ok(requests) = self.requests.get_mut() {
            requests.take();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl KvNodeHandle {
    pub fn start_one_node(
        node_id: NodeId,
        data_file: impl AsRef<Path>,
        queue_capacity: usize,
    ) -> Result<Self, NodeError> {
        if queue_capacity == 0 {
            return Err(NodeError::InvalidQueueCapacity);
        }
        let data_file = data_file.as_ref();
        if let Some(parent) = data_file.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|error| NodeError::Startup(error.to_string()))?;
        }
        let storage = SqliteStorage::open(data_file)
            .map_err(|error| NodeError::Startup(error.to_string()))?;
        let voters =
            VoterSet::new([node_id]).map_err(|error| NodeError::Startup(error.to_string()))?;
        let raft = DurableRaft::recover(node_id, voters, storage)
            .map_err(|error| NodeError::Startup(error.to_string()))?;
        Self::start_worker(raft, queue_capacity)
    }

    pub async fn execute(&self, command: Command) -> Result<CommandResult, ServiceError> {
        let (respond, response) = oneshot::channel();
        let requests = self
            .inner
            .requests
            .lock()
            .map_err(|_| ServiceError::Internal)?
            .clone()
            .ok_or(ServiceError::Unavailable)?;
        requests
            .try_send(Request { command, respond })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ServiceError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => ServiceError::Unavailable,
            })?;
        response
            .await
            .map_err(|_| ServiceError::Unavailable)?
            .map_err(|_| ServiceError::Internal)
    }

    fn start_worker(
        raft: DurableRaft<SqliteStorage>,
        queue_capacity: usize,
    ) -> Result<Self, NodeError> {
        let (requests, mut receiver) = mpsc::channel::<Request>(queue_capacity);
        let (started, startup) = std_mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("raft-kv-node".to_owned())
            .spawn(move || {
                let mut worker = Worker { raft };
                let startup_result = worker.bootstrap().map_err(|error| error.to_string());
                if started.send(startup_result).is_err() {
                    return;
                }
                while let Some(request) = receiver.blocking_recv() {
                    let result = worker.execute(request.command);
                    let _ = request.respond.send(result);
                }
            })
            .map_err(|error| NodeError::Startup(error.to_string()))?;
        startup
            .recv()
            .map_err(|_| NodeError::WorkerStopped)?
            .map_err(NodeError::Startup)?;
        Ok(Self {
            inner: Arc::new(KvNodeInner {
                requests: Mutex::new(Some(requests)),
                worker: Mutex::new(Some(worker)),
            }),
        })
    }
}

struct Request {
    command: Command,
    respond: oneshot::Sender<Result<CommandResult, NodeError>>,
}

struct Worker {
    raft: DurableRaft<SqliteStorage>,
}

impl Worker {
    fn bootstrap(&mut self) -> Result<(), NodeError> {
        let effects = self
            .raft
            .handle(Event::ElectionTimeout)
            .map_err(|error| NodeError::Runtime(error.to_string()))?;
        self.consume(effects)?;
        if !self.raft.core().leader_ready() {
            return Err(NodeError::LeaderNotReady);
        }
        Ok(())
    }

    fn execute(&mut self, command: Command) -> Result<CommandResult, NodeError> {
        let encoded =
            encode_command(&command).map_err(|error| NodeError::Runtime(error.to_string()))?;
        let (index, effects) = self
            .raft
            .propose(EntryPayload::Command(encoded))
            .map_err(|error| NodeError::Runtime(error.to_string()))?;
        self.consume(effects)?
            .into_iter()
            .find(|applied| applied.index == index)
            .and_then(|applied| applied.result)
            .ok_or(NodeError::MissingResult(index))
    }

    fn consume(
        &mut self,
        effects: Vec<Effect>,
    ) -> Result<Vec<crate::storage::AppliedEntry>, NodeError> {
        let mut pending = effects;
        let mut applied = Vec::new();
        while let Some(effect) = pending.pop() {
            match effect {
                Effect::ApplyCommitted(entries) => {
                    let batch = self
                        .raft
                        .apply_committed(&entries)
                        .map_err(|error| NodeError::Runtime(error.to_string()))?;
                    applied.extend(batch.entries);
                    pending.extend(batch.effects);
                }
                Effect::ResetElectionTimer | Effect::CommitIndexAdvanced(_) => {}
                Effect::Send(_) | Effect::SendSnapshot { .. } => {
                    return Err(NodeError::UnexpectedNetworkEffect);
                }
                Effect::PersistHardState(_) | Effect::PersistLog(_) => {
                    return Err(NodeError::LeakedPersistenceEffect);
                }
            }
        }
        Ok(applied)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    InvalidQueueCapacity,
    Startup(String),
    WorkerStopped,
    LeaderNotReady,
    MissingResult(u64),
    UnexpectedNetworkEffect,
    LeakedPersistenceEffect,
    Runtime(String),
}

impl Display for NodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "KV node failure: {self:?}")
    }
}

impl Error for NodeError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;
    use crate::kv::{ClientId, Mutation, MutationCommand, MutationResult};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn temp_database() -> std::path::PathBuf {
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("raft-kv-node-{}-{suffix}", std::process::id()));
        fs::create_dir_all(&directory).expect("create directory");
        directory.join("state.sqlite3")
    }

    #[tokio::test]
    async fn one_node_requests_cross_raft_commit_and_survive_restart() {
        let database = temp_database();
        let node = KvNodeHandle::start_one_node(NodeId(1), &database, 8).expect("start node");
        let create = Command::Mutate(MutationCommand {
            client_id: ClientId([1; 16]),
            sequence: 1,
            mutation: Mutation::Create {
                key: "key".into(),
                value: b"value".to_vec(),
            },
        });
        assert!(matches!(
            node.execute(create.clone()).await.expect("create"),
            CommandResult::Mutation(MutationResult::Created { .. })
        ));
        drop(node);

        let restarted = KvNodeHandle::start_one_node(NodeId(1), &database, 8).expect("restart");
        assert!(matches!(
            restarted.execute(create).await.expect("retry"),
            CommandResult::Mutation(MutationResult::Duplicate(_))
        ));
        let read = restarted
            .execute(Command::Read { key: "key".into() })
            .await
            .expect("read");
        assert!(
            matches!(read, CommandResult::Read(crate::kv::ReadResult::Found { value, .. }) if value == b"value")
        );
    }
}
