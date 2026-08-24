use std::{
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    sync::atomic::{AtomicU64, Ordering},
};

use raft_kv_service::{
    kv::{
        ClientId, Command, CommandResult, Mutation, MutationCommand, MutationResult, ValueRecord,
        encode_command,
    },
    raft::{EntryPayload, HardState, LogEntry, LogPosition, NodeId, Term},
    storage::{AppendOutcome, RaftStorage, SnapshotStorage, SqliteStorage, StateMachineStorage},
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn temp_database(label: &str) -> PathBuf {
    let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let directory =
        std::env::temp_dir().join(format!("raft-kv-{label}-{}-{suffix}", std::process::id()));
    std::fs::create_dir(&directory).expect("create exact test directory");
    directory.join("node.sqlite3")
}

fn command_entry(index: u64) -> (Command, LogEntry) {
    let command = Command::Mutate(MutationCommand {
        client_id: ClientId([9; 16]),
        sequence: 1,
        mutation: Mutation::Create {
            key: "durable".into(),
            value: b"state".to_vec(),
        },
    });
    let entry = LogEntry {
        index,
        term: Term(2),
        payload: EntryPayload::Command(encode_command(&command).expect("encode command")),
    };
    (command, entry)
}

fn exercise_log_contract<S>(storage: &mut S)
where
    S: RaftStorage,
    S::Error: std::fmt::Debug,
{
    let hard_state = HardState {
        current_term: Term(2),
        voted_for: Some(NodeId(1)),
    };
    storage
        .persist_hard_state(hard_state)
        .expect("persist hard state");
    assert_eq!(
        storage.load_hard_state().expect("load hard state"),
        hard_state
    );
    let noop = LogEntry {
        index: 1,
        term: Term(2),
        payload: EntryPayload::Noop,
    };
    storage.append_local(&noop).expect("append local entry");
    assert_eq!(storage.entry(1).expect("load entry"), Some(noop));
    assert_eq!(
        storage
            .append_from_leader(
                LogPosition {
                    index: 1,
                    term: Term(1),
                },
                &[],
            )
            .expect("mismatch is a protocol outcome"),
        AppendOutcome::Rejected {
            local_term: Some(Term(2)),
        }
    );
}

#[test]
fn sqlite_satisfies_the_reusable_log_contract() {
    let mut storage = SqliteStorage::open_in_memory().expect("open storage");
    exercise_log_contract(&mut storage);
}

#[test]
fn abrupt_exit_after_commit_recovers_atomic_state_and_deduplication() {
    let database = temp_database("crash-contract");
    let status = ProcessCommand::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "crash_writer_process", "--nocapture"])
        .env("RAFT_KV_CRASH_TEST_DATABASE", &database)
        .status()
        .expect("run crash writer");
    assert_eq!(
        status.code(),
        Some(91),
        "writer must exit without destructors"
    );

    let mut storage = SqliteStorage::open(&database).expect("recover storage");
    assert_eq!(storage.last_applied().expect("last applied"), 2);
    assert_eq!(
        storage.value("durable").expect("recovered value"),
        Some(ValueRecord {
            value: b"state".to_vec(),
            revision: 2,
        })
    );

    let (_, retry) = command_entry(3);
    storage.append_local(&retry).expect("append retry");
    assert_eq!(
        storage.apply_committed(&[retry]).expect("apply retry")[0].result,
        Some(CommandResult::Mutation(MutationResult::Duplicate(
            Box::new(MutationResult::Created { revision: 2 })
        )))
    );
    let directory = database.parent().expect("database directory");
    std::fs::remove_dir_all(directory).expect("remove exact test directory");
}

#[test]
fn abrupt_exit_after_snapshot_publication_recovers_compacted_state() {
    let database = temp_database("snapshot-crash-contract");
    let status = ProcessCommand::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "snapshot_crash_writer_process", "--nocapture"])
        .env("RAFT_KV_SNAPSHOT_CRASH_DATABASE", &database)
        .status()
        .expect("run snapshot crash writer");
    assert_eq!(
        status.code(),
        Some(92),
        "writer must exit without destructors"
    );

    let storage = SqliteStorage::open(&database).expect("recover snapshotted storage");
    let snapshot = storage
        .current_snapshot()
        .expect("load snapshot")
        .expect("published snapshot");
    assert_eq!(snapshot.last_included.index, 2);
    assert_eq!(storage.last_applied().expect("last applied"), 2);
    assert!(
        storage
            .entries_from(1, 10)
            .expect("compacted log")
            .is_empty()
    );
    assert_eq!(
        storage.value("durable").expect("recovered value"),
        Some(ValueRecord {
            value: b"state".to_vec(),
            revision: 2,
        })
    );
    std::fs::remove_dir_all(database.parent().expect("database directory"))
        .expect("remove exact test directory");
}

#[test]
fn crash_writer_process() {
    let Ok(database) = std::env::var("RAFT_KV_CRASH_TEST_DATABASE") else {
        return;
    };
    write_committed_state(Path::new(&database));
    std::process::exit(91);
}

#[test]
fn snapshot_crash_writer_process() {
    let Ok(database) = std::env::var("RAFT_KV_SNAPSHOT_CRASH_DATABASE") else {
        return;
    };
    write_committed_state(Path::new(&database));
    let mut storage = SqliteStorage::open(&database).expect("reopen crash database");
    storage
        .create_snapshot([1; 16], &[NodeId(1)])
        .expect("publish snapshot");
    std::process::exit(92);
}

fn write_committed_state(database: &Path) {
    let mut storage = SqliteStorage::open(database).expect("open crash database");
    let noop = LogEntry {
        index: 1,
        term: Term(2),
        payload: EntryPayload::Noop,
    };
    let (_, create) = command_entry(2);
    storage.append_local(&noop).expect("append noop");
    storage.append_local(&create).expect("append create");
    storage
        .apply_committed(&[noop, create])
        .expect("durably apply batch");
}
