use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    time::Duration,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use sha2::{Digest, Sha256};

use super::{
    AppendOutcome, AppliedEntry, RaftStorage, SNAPSHOT_FORMAT_VERSION, Snapshot, SnapshotStorage,
    StateMachineStorage,
};
use crate::{
    kv::{
        ClientId, Command, CommandError, CommandResult, Mutation, MutationResult, ReadResult,
        ValueRecord, decode_command, decode_command_result, decode_mutation_result,
        encode_command_result, encode_mutation_result, fingerprint, validate_key,
    },
    raft::{EntryPayload, HardState, LogEntry, LogPosition, NodeId, Term},
};

const SCHEMA_VERSION: i64 = 3;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_CLIENTS: u64 = 10_000;

pub struct SqliteStorage {
    connection: Connection,
}

impl SqliteStorage {
    pub fn validate_snapshot_for_cluster(
        &self,
        expected_cluster_id: [u8; 16],
        snapshot: &Snapshot,
    ) -> Result<(), StorageError> {
        if snapshot.cluster_id != expected_cluster_id {
            return Err(StorageError::SnapshotClusterMismatch);
        }
        let _ = decode_snapshot(snapshot)?;
        Ok(())
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_max_clients(path, DEFAULT_MAX_CLIENTS)
    }

    pub fn open_with_max_clients(
        path: impl AsRef<Path>,
        max_clients: u64,
    ) -> Result<Self, StorageError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Self::initialize(connection, true, max_clients)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::initialize(Connection::open_in_memory()?, false, DEFAULT_MAX_CLIENTS)
    }

    fn initialize(
        connection: Connection,
        file_backed: bool,
        max_clients: u64,
    ) -> Result<Self, StorageError> {
        if max_clients == 0 {
            return Err(StorageError::InvalidMaxClients(max_clients));
        }
        connection.busy_timeout(BUSY_TIMEOUT)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "trusted_schema", false)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "fullfsync", true)?;
        connection.pragma_update(None, "checkpoint_fullfsync", true)?;
        if file_backed {
            let journal_mode: String =
                connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            if !journal_mode.eq_ignore_ascii_case("wal") {
                connection.pragma_update(None, "journal_mode", "WAL")?;
            }
            connection.pragma_update(None, "wal_autocheckpoint", 1_000_i64)?;
        }

        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS hard_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                current_term BLOB NOT NULL CHECK (length(current_term) = 8),
                voted_for BLOB CHECK (voted_for IS NULL OR length(voted_for) = 8)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS raft_log (
                log_index BLOB PRIMARY KEY CHECK (length(log_index) = 8),
                term BLOB NOT NULL CHECK (length(term) = 8),
                entry_kind INTEGER NOT NULL CHECK (entry_kind IN (0, 1)),
                payload BLOB NOT NULL,
                CHECK (entry_kind != 0 OR length(payload) = 0)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS state_machine_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                last_applied BLOB NOT NULL CHECK (length(last_applied) = 8),
                max_clients BLOB NOT NULL CHECK (length(max_clients) = 8)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS kv_state (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL,
                revision BLOB NOT NULL CHECK (length(revision) = 8)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS client_sessions (
                client_id BLOB PRIMARY KEY CHECK (length(client_id) = 16),
                sequence BLOB NOT NULL CHECK (length(sequence) = 8),
                fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32),
                result BLOB NOT NULL
            ) STRICT;

            CREATE TABLE IF NOT EXISTS applied_results (
                log_index BLOB PRIMARY KEY CHECK (length(log_index) = 8),
                result BLOB
            ) STRICT;

            CREATE TABLE IF NOT EXISTS raft_snapshot (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                format_version INTEGER NOT NULL,
                cluster_id BLOB NOT NULL CHECK (length(cluster_id) = 16),
                last_included_index BLOB NOT NULL CHECK (length(last_included_index) = 8),
                last_included_term BLOB NOT NULL CHECK (length(last_included_term) = 8),
                payload BLOB NOT NULL,
                digest BLOB NOT NULL CHECK (length(digest) = 32)
            ) STRICT;

            INSERT OR IGNORE INTO hard_state(singleton, current_term, voted_for)
            VALUES (1, X'0000000000000000', NULL);
            ",
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO state_machine_meta(singleton, last_applied, max_clients)
             VALUES (1, ?1, ?2)",
            params![encode_u64(0).as_slice(), encode_u64(max_clients).as_slice()],
        )?;

        let user_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match user_version {
            0..=2 => connection.pragma_update(None, "user_version", SCHEMA_VERSION)?,
            SCHEMA_VERSION => {}
            found => {
                return Err(StorageError::UnsupportedSchemaVersion {
                    expected: SCHEMA_VERSION,
                    found,
                });
            }
        }

        let storage = Self { connection };
        storage.validate()?;
        let (_, persisted_max_clients) = load_state_machine_meta(&storage.connection)?;
        if persisted_max_clients != max_clients {
            return Err(StorageError::MaxClientsMismatch {
                configured: max_clients,
                persisted: persisted_max_clients,
            });
        }
        Ok(storage)
    }

    fn validate(&self) -> Result<(), StorageError> {
        let quick_check: String =
            self.connection
                .pragma_query_value(None, "quick_check", |row| row.get(0))?;
        if quick_check != "ok" {
            return Err(StorageError::IntegrityCheckFailed(quick_check));
        }

        self.load_hard_state()?;
        let (last_applied, _) = load_state_machine_meta(&self.connection)?;
        let snapshot_position = load_snapshot(&self.connection)?
            .map_or(LogPosition::default(), |snapshot| snapshot.last_included);
        let last_log = self.last_log_position()?.index;
        if last_applied < snapshot_position.index || last_applied > last_log {
            return Err(StorageError::AppliedBeyondLog {
                last_applied,
                last_log,
            });
        }
        let mut expected_index = snapshot_position
            .index
            .checked_add(1)
            .ok_or(StorageError::IndexExhausted)?;
        for entry in self.entries_from(expected_index, usize::MAX)? {
            if entry.index != expected_index {
                return Err(StorageError::NonContiguousLog {
                    expected: expected_index,
                    found: entry.index,
                });
            }
            expected_index = expected_index
                .checked_add(1)
                .ok_or(StorageError::IndexExhausted)?;
        }
        Ok(())
    }
}

impl RaftStorage for SqliteStorage {
    type Error = StorageError;

    fn load_hard_state(&self) -> Result<HardState, Self::Error> {
        load_hard_state(&self.connection)
    }

    fn persist_hard_state(&mut self, hard_state: HardState) -> Result<(), Self::Error> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_hard_state(&transaction)?;
        validate_hard_state_transition(current, hard_state)?;
        transaction.execute(
            "UPDATE hard_state SET current_term = ?1, voted_for = ?2 WHERE singleton = 1",
            params![
                encode_u64(hard_state.current_term.0).as_slice(),
                hard_state.voted_for.map(|node| encode_u64(node.0).to_vec())
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn last_log_position(&self) -> Result<LogPosition, Self::Error> {
        last_log_position(&self.connection)
    }

    fn entry(&self, index: u64) -> Result<Option<LogEntry>, Self::Error> {
        load_entry(&self.connection, index)
    }

    fn entries_from(&self, start: u64, limit: usize) -> Result<Vec<LogEntry>, Self::Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = self.connection.prepare(
            "SELECT log_index, term, entry_kind, payload
             FROM raft_log WHERE log_index >= ?1
             ORDER BY log_index ASC LIMIT ?2",
        )?;
        let entries = statement
            .query_map(
                params![encode_u64(start).as_slice(), sql_limit],
                decode_entry,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    fn append_local(&mut self, entry: &LogEntry) -> Result<(), Self::Error> {
        validate_entry(entry)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let last = last_log_position(&transaction)?;
        let expected = last
            .index
            .checked_add(1)
            .ok_or(StorageError::IndexExhausted)?;
        if entry.index != expected {
            return Err(StorageError::NonContiguousLog {
                expected,
                found: entry.index,
            });
        }
        insert_entry(&transaction, entry)?;
        transaction.commit()?;
        Ok(())
    }

    fn append_from_leader(
        &mut self,
        previous: LogPosition,
        entries: &[LogEntry],
    ) -> Result<AppendOutcome, Self::Error> {
        validate_entry_sequence(previous.index, entries)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let local_previous_term = term_at(&transaction, previous.index)?;
        if local_previous_term != Some(previous.term) {
            return Ok(AppendOutcome::Rejected {
                local_term: local_previous_term,
            });
        }

        let mut replace_from = None;
        for entry in entries {
            match load_entry(&transaction, entry.index)? {
                Some(local) if local == *entry => {}
                Some(_) => {
                    replace_from = Some(entry.index);
                    break;
                }
                None => {
                    replace_from = Some(entry.index);
                    break;
                }
            }
        }

        if let Some(first_replacement) = replace_from {
            transaction.execute(
                "DELETE FROM raft_log WHERE log_index >= ?1",
                params![encode_u64(first_replacement).as_slice()],
            )?;
            for entry in entries
                .iter()
                .skip_while(|entry| entry.index < first_replacement)
            {
                insert_entry(&transaction, entry)?;
            }
        }

        let last_index = entries.last().map_or(previous.index, |entry| entry.index);
        transaction.commit()?;
        Ok(AppendOutcome::Appended { last_index })
    }
}

impl StateMachineStorage for SqliteStorage {
    fn last_applied(&self) -> Result<u64, Self::Error> {
        load_state_machine_meta(&self.connection).map(|(last_applied, _)| last_applied)
    }

    fn apply_committed(&mut self, entries: &[LogEntry]) -> Result<Vec<AppliedEntry>, Self::Error> {
        if let Some(first) = entries.first() {
            validate_entry_sequence(
                first
                    .index
                    .checked_sub(1)
                    .ok_or(StorageError::IndexExhausted)?,
                entries,
            )?;
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (mut last_applied, max_clients) = load_state_machine_meta(&transaction)?;
        let mut applied = Vec::with_capacity(entries.len());
        for entry in entries {
            if load_entry(&transaction, entry.index)?.as_ref() != Some(entry) {
                return Err(StorageError::ApplicationLogMismatch { index: entry.index });
            }
            if entry.index <= last_applied {
                let result = load_applied_result(&transaction, entry.index)?
                    .ok_or(StorageError::MissingAppliedResult { index: entry.index })?;
                applied.push(AppliedEntry {
                    index: entry.index,
                    result,
                });
                continue;
            }
            let expected = last_applied
                .checked_add(1)
                .ok_or(StorageError::IndexExhausted)?;
            if entry.index != expected {
                return Err(StorageError::NonContiguousApplication {
                    expected,
                    found: entry.index,
                });
            }
            let result = match &entry.payload {
                EntryPayload::Noop => None,
                EntryPayload::Command(encoded) => {
                    let command = decode_command(encoded).map_err(StorageError::Command)?;
                    Some(apply_command(
                        &transaction,
                        entry.index,
                        &command,
                        max_clients,
                    )?)
                }
            };
            let encoded_result = result
                .as_ref()
                .map(encode_command_result)
                .transpose()
                .map_err(StorageError::Command)?;
            transaction.execute(
                "INSERT INTO applied_results(log_index, result) VALUES (?1, ?2)",
                params![encode_u64(entry.index).as_slice(), encoded_result],
            )?;
            last_applied = entry.index;
            transaction.execute(
                "UPDATE state_machine_meta SET last_applied = ?1 WHERE singleton = 1",
                params![encode_u64(last_applied).as_slice()],
            )?;
            applied.push(AppliedEntry {
                index: entry.index,
                result,
            });
        }
        transaction.commit()?;
        Ok(applied)
    }

    fn value(&self, key: &str) -> Result<Option<ValueRecord>, Self::Error> {
        load_value(&self.connection, key)
    }
}

impl SnapshotStorage for SqliteStorage {
    fn current_snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        load_snapshot(&self.connection)
    }

    fn create_snapshot(
        &mut self,
        cluster_id: [u8; 16],
        voters: &[NodeId],
    ) -> Result<Snapshot, Self::Error> {
        validate_voters(voters)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (last_applied, max_clients) = load_state_machine_meta(&transaction)?;
        if last_applied == 0 {
            return Err(StorageError::SnapshotAppliedEntryMissingTerm(0));
        }
        let last_included = LogPosition {
            index: last_applied,
            term: term_at(&transaction, last_applied)?
                .ok_or(StorageError::SnapshotAppliedEntryMissingTerm(last_applied))?,
        };
        if let Some(current) = load_snapshot(&transaction)?
            && current.last_included.index >= last_applied
        {
            return Ok(current);
        }
        let payload =
            encode_snapshot_payload(&transaction, cluster_id, last_included, voters, max_clients)?;
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let snapshot = Snapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            cluster_id,
            last_included,
            voters: voters.to_vec(),
            payload,
            digest,
        };
        persist_snapshot(&transaction, &snapshot)?;
        transaction.execute(
            "DELETE FROM raft_log WHERE log_index <= ?1",
            params![encode_u64(last_applied).as_slice()],
        )?;
        transaction.execute(
            "DELETE FROM applied_results WHERE log_index <= ?1",
            params![encode_u64(last_applied).as_slice()],
        )?;
        transaction.commit()?;
        Ok(snapshot)
    }

    fn install_snapshot(
        &mut self,
        expected_cluster_id: [u8; 16],
        snapshot: &Snapshot,
    ) -> Result<(), Self::Error> {
        self.validate_snapshot_for_cluster(expected_cluster_id, snapshot)?;
        let decoded = decode_snapshot(snapshot)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (last_applied, configured_max_clients) = load_state_machine_meta(&transaction)?;
        if snapshot.last_included.index < last_applied {
            return Err(StorageError::OlderSnapshot {
                received: snapshot.last_included.index,
                applied: last_applied,
            });
        }
        if decoded.max_clients != configured_max_clients {
            return Err(StorageError::MaxClientsMismatch {
                configured: configured_max_clients,
                persisted: decoded.max_clients,
            });
        }
        if let Some(current) = load_snapshot(&transaction)?
            && current.last_included.index == snapshot.last_included.index
        {
            if current.digest == snapshot.digest {
                return Ok(());
            }
            return Err(StorageError::SnapshotConflict(snapshot.last_included.index));
        }

        let retain_suffix = term_at(&transaction, snapshot.last_included.index)?
            == Some(snapshot.last_included.term);
        if retain_suffix {
            transaction.execute(
                "DELETE FROM raft_log WHERE log_index <= ?1",
                params![encode_u64(snapshot.last_included.index).as_slice()],
            )?;
        } else {
            transaction.execute("DELETE FROM raft_log", [])?;
        }
        transaction.execute("DELETE FROM kv_state", [])?;
        transaction.execute("DELETE FROM client_sessions", [])?;
        transaction.execute("DELETE FROM applied_results", [])?;
        for row in decoded.keys {
            transaction.execute(
                "INSERT INTO kv_state(key, value, revision) VALUES (?1, ?2, ?3)",
                params![row.key, row.value, encode_u64(row.revision).as_slice()],
            )?;
        }
        for row in decoded.clients {
            transaction.execute(
                "INSERT INTO client_sessions(client_id, sequence, fingerprint, result)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    row.client_id.as_slice(),
                    encode_u64(row.sequence).as_slice(),
                    row.fingerprint.as_slice(),
                    row.result,
                ],
            )?;
        }
        transaction.execute(
            "UPDATE state_machine_meta SET last_applied = ?1 WHERE singleton = 1",
            params![encode_u64(snapshot.last_included.index).as_slice()],
        )?;
        persist_snapshot(&transaction, snapshot)?;
        transaction.commit()?;
        Ok(())
    }
}

#[derive(Debug)]
struct SnapshotKey {
    key: String,
    value: Vec<u8>,
    revision: u64,
}

#[derive(Debug)]
struct SnapshotClient {
    client_id: [u8; 16],
    sequence: u64,
    fingerprint: [u8; 32],
    result: Vec<u8>,
}

#[derive(Debug)]
struct DecodedSnapshot {
    max_clients: u64,
    keys: Vec<SnapshotKey>,
    clients: Vec<SnapshotClient>,
}

const SNAPSHOT_MAGIC: &[u8; 8] = b"RKVSNP01";

fn validate_voters(voters: &[NodeId]) -> Result<(), StorageError> {
    if voters.is_empty()
        || voters.iter().any(|node| node.0 == 0)
        || voters.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(StorageError::InvalidSnapshotVoters);
    }
    Ok(())
}

fn encode_snapshot_payload(
    connection: &Connection,
    cluster_id: [u8; 16],
    last_included: LogPosition,
    voters: &[NodeId],
    max_clients: u64,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Vec::new();
    output.extend_from_slice(SNAPSHOT_MAGIC);
    put_u32(&mut output, SNAPSHOT_FORMAT_VERSION);
    output.extend_from_slice(&cluster_id);
    put_u64(&mut output, last_included.index);
    put_u64(&mut output, last_included.term.0);
    put_u64(
        &mut output,
        u64::try_from(voters.len()).map_err(|_| StorageError::SnapshotEncoding)?,
    );
    for voter in voters {
        put_u64(&mut output, voter.0);
    }
    put_u64(&mut output, max_clients);

    let mut statement =
        connection.prepare("SELECT key, value, revision FROM kv_state ORDER BY key ASC")?;
    let keys = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    put_u64(
        &mut output,
        u64::try_from(keys.len()).map_err(|_| StorageError::SnapshotEncoding)?,
    );
    for (key, value, revision) in keys {
        put_bytes(&mut output, key.as_bytes())?;
        put_bytes(&mut output, &value)?;
        put_u64(&mut output, decode_u64("snapshot revision", &revision)?);
    }

    let mut statement = connection.prepare(
        "SELECT client_id, sequence, fingerprint, result
         FROM client_sessions ORDER BY client_id ASC",
    )?;
    let clients = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    put_u64(
        &mut output,
        u64::try_from(clients.len()).map_err(|_| StorageError::SnapshotEncoding)?,
    );
    for (client_id, sequence, fingerprint, result) in clients {
        let client_id: [u8; 16] = client_id
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        let fingerprint: [u8; 32] = fingerprint
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        output.extend_from_slice(&client_id);
        put_u64(&mut output, decode_u64("snapshot sequence", &sequence)?);
        output.extend_from_slice(&fingerprint);
        put_bytes(&mut output, &result)?;
    }
    Ok(output)
}

fn decode_snapshot(snapshot: &Snapshot) -> Result<DecodedSnapshot, StorageError> {
    if snapshot.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(StorageError::UnsupportedSnapshotFormat(
            snapshot.format_version,
        ));
    }
    if Sha256::digest(&snapshot.payload).as_slice() != snapshot.digest {
        return Err(StorageError::SnapshotDigestMismatch);
    }
    let mut input = SnapshotReader::new(&snapshot.payload);
    if input.take(SNAPSHOT_MAGIC.len())? != SNAPSHOT_MAGIC {
        return Err(StorageError::SnapshotEncoding);
    }
    let format = input.u32()?;
    let cluster_id: [u8; 16] = input
        .take(16)?
        .try_into()
        .map_err(|_| StorageError::SnapshotEncoding)?;
    let position = LogPosition {
        index: input.u64()?,
        term: Term(input.u64()?),
    };
    let voter_count = input.count()?;
    let mut voters = Vec::new();
    for _ in 0..voter_count {
        voters.push(NodeId(input.u64()?));
    }
    validate_voters(&voters)?;
    if format != snapshot.format_version
        || cluster_id != snapshot.cluster_id
        || position != snapshot.last_included
        || voters != snapshot.voters
        || position.index == 0
        || position.term == Term(0)
    {
        return Err(StorageError::SnapshotEncoding);
    }
    let max_clients = input.u64()?;
    let key_count = input.count()?;
    let mut keys = Vec::new();
    for _ in 0..key_count {
        let key = String::from_utf8(input.bytes()?).map_err(|_| StorageError::SnapshotEncoding)?;
        validate_key(&key).map_err(StorageError::Command)?;
        if keys
            .last()
            .is_some_and(|previous: &SnapshotKey| previous.key.as_str() >= key.as_str())
        {
            return Err(StorageError::SnapshotEncoding);
        }
        let value = input.bytes()?;
        let revision = input.u64()?;
        if revision == 0 || revision > position.index {
            return Err(StorageError::SnapshotEncoding);
        }
        keys.push(SnapshotKey {
            key,
            value,
            revision,
        });
    }
    let client_count = input.count()?;
    let mut clients = Vec::new();
    for _ in 0..client_count {
        let client_id = input
            .take(16)?
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        let sequence = input.u64()?;
        let fingerprint = input
            .take(32)?
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        let result = input.bytes()?;
        decode_mutation_result(&result).map_err(StorageError::Command)?;
        if sequence == 0
            || clients
                .last()
                .is_some_and(|previous: &SnapshotClient| previous.client_id >= client_id)
        {
            return Err(StorageError::SnapshotEncoding);
        }
        clients.push(SnapshotClient {
            client_id,
            sequence,
            fingerprint,
            result,
        });
    }
    if !input.is_empty() || u64::try_from(clients.len()).unwrap_or(u64::MAX) > max_clients {
        return Err(StorageError::SnapshotEncoding);
    }
    Ok(DecodedSnapshot {
        max_clients,
        keys,
        clients,
    })
}

fn persist_snapshot(connection: &Connection, snapshot: &Snapshot) -> Result<(), StorageError> {
    connection.execute(
        "INSERT OR REPLACE INTO raft_snapshot(
             singleton, format_version, cluster_id, last_included_index,
             last_included_term, payload, digest
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            i64::from(snapshot.format_version),
            snapshot.cluster_id.as_slice(),
            encode_u64(snapshot.last_included.index).as_slice(),
            encode_u64(snapshot.last_included.term.0).as_slice(),
            snapshot.payload,
            snapshot.digest.as_slice(),
        ],
    )?;
    Ok(())
}

fn load_snapshot(connection: &Connection) -> Result<Option<Snapshot>, StorageError> {
    let row = connection
        .query_row(
            "SELECT format_version, cluster_id, last_included_index,
                    last_included_term, payload, digest
             FROM raft_snapshot WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )
        .optional()?;
    row.map(|(format, cluster_id, index, term, payload, digest)| {
        let format_version = u32::try_from(format).map_err(|_| StorageError::SnapshotEncoding)?;
        let cluster_id = cluster_id
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        let digest = digest
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        let snapshot = Snapshot {
            format_version,
            cluster_id,
            last_included: LogPosition {
                index: decode_u64("snapshot index", &index)?,
                term: Term(decode_u64("snapshot term", &term)?),
            },
            voters: Vec::new(),
            payload,
            digest,
        };
        let decoded_voters = decode_snapshot_voters(&snapshot)?;
        let snapshot = Snapshot {
            voters: decoded_voters,
            ..snapshot
        };
        let _ = decode_snapshot(&snapshot)?;
        Ok(snapshot)
    })
    .transpose()
}

fn decode_snapshot_voters(snapshot: &Snapshot) -> Result<Vec<NodeId>, StorageError> {
    if Sha256::digest(&snapshot.payload).as_slice() != snapshot.digest {
        return Err(StorageError::SnapshotDigestMismatch);
    }
    let mut input = SnapshotReader::new(&snapshot.payload);
    if input.take(8)? != SNAPSHOT_MAGIC {
        return Err(StorageError::SnapshotEncoding);
    }
    let _ = input.u32()?;
    let _ = input.take(16)?;
    let _ = input.u64()?;
    let _ = input.u64()?;
    let count = input.count()?;
    let mut voters = Vec::new();
    for _ in 0..count {
        voters.push(NodeId(input.u64()?));
    }
    Ok(voters)
}

struct SnapshotReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(StorageError::SnapshotEncoding)?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        let bytes = self
            .take(4)?
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        let bytes = self
            .take(8)?
            .try_into()
            .map_err(|_| StorageError::SnapshotEncoding)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn count(&mut self) -> Result<u64, StorageError> {
        let count = self.u64()?;
        if count > u64::try_from(self.bytes.len()).unwrap_or(u64::MAX) {
            return Err(StorageError::SnapshotEncoding);
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, StorageError> {
        let length = usize::try_from(self.u64()?).map_err(|_| StorageError::SnapshotEncoding)?;
        Ok(self.take(length)?.to_vec())
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), StorageError> {
    put_u64(
        output,
        u64::try_from(value.len()).map_err(|_| StorageError::SnapshotEncoding)?,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn apply_command(
    transaction: &Transaction<'_>,
    revision: u64,
    command: &Command,
    max_clients: u64,
) -> Result<CommandResult, StorageError> {
    match command {
        Command::Read { key } => Ok(CommandResult::Read(load_value(transaction, key)?.map_or(
            ReadResult::NotFound,
            |record| ReadResult::Found {
                value: record.value,
                revision: record.revision,
            },
        ))),
        Command::Mutate(command) => {
            let fingerprint = fingerprint(&command.mutation).map_err(StorageError::Command)?;
            if let Some((latest, prior_fingerprint, prior_result)) =
                load_client(transaction, command.client_id)?
            {
                if command.sequence < latest {
                    return Ok(CommandResult::Mutation(MutationResult::StaleSequence {
                        latest,
                    }));
                }
                if command.sequence == latest {
                    return Ok(CommandResult::Mutation(
                        if fingerprint == prior_fingerprint {
                            MutationResult::Duplicate(Box::new(prior_result))
                        } else {
                            MutationResult::IdentityConflict {
                                sequence: command.sequence,
                            }
                        },
                    ));
                }
                let expected = latest
                    .checked_add(1)
                    .ok_or(StorageError::Command(CommandError::SequenceExhausted))?;
                if command.sequence != expected {
                    return Ok(CommandResult::Mutation(MutationResult::SequenceGap {
                        expected,
                    }));
                }
            } else {
                if command.sequence != 1 {
                    return Ok(CommandResult::Mutation(MutationResult::SequenceGap {
                        expected: 1,
                    }));
                }
                let client_count: i64 =
                    transaction
                        .query_row("SELECT COUNT(*) FROM client_sessions", [], |row| row.get(0))?;
                let client_count = u64::try_from(client_count)
                    .map_err(|_| StorageError::InvalidClientCount(client_count))?;
                if client_count >= max_clients {
                    return Ok(CommandResult::Mutation(
                        MutationResult::ClientCapacityReached,
                    ));
                }
            }

            let result = execute_mutation(transaction, revision, &command.mutation)?;
            transaction.execute(
                "INSERT INTO client_sessions(client_id, sequence, fingerprint, result)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(client_id) DO UPDATE SET
                   sequence = excluded.sequence,
                   fingerprint = excluded.fingerprint,
                   result = excluded.result",
                params![
                    command.client_id.0.as_slice(),
                    encode_u64(command.sequence).as_slice(),
                    fingerprint.as_slice(),
                    encode_mutation_result(&result).map_err(StorageError::Command)?,
                ],
            )?;
            Ok(CommandResult::Mutation(result))
        }
    }
}

fn execute_mutation(
    transaction: &Transaction<'_>,
    revision: u64,
    mutation: &Mutation,
) -> Result<MutationResult, StorageError> {
    match mutation {
        Mutation::Create { key, value } => {
            if let Some(current) = load_value(transaction, key)? {
                return Ok(MutationResult::PreconditionFailed {
                    current_revision: Some(current.revision),
                });
            }
            write_value(transaction, key, value, revision)?;
            Ok(MutationResult::Created { revision })
        }
        Mutation::Update {
            key,
            value,
            expected_revision,
        } => {
            let Some(current) = load_value(transaction, key)? else {
                return Ok(MutationResult::PreconditionFailed {
                    current_revision: None,
                });
            };
            if expected_revision.is_some_and(|expected| expected != current.revision) {
                return Ok(MutationResult::PreconditionFailed {
                    current_revision: Some(current.revision),
                });
            }
            write_value(transaction, key, value, revision)?;
            Ok(MutationResult::Updated { revision })
        }
        Mutation::Delete {
            key,
            expected_revision,
        } => {
            let Some(current) = load_value(transaction, key)? else {
                return Ok(MutationResult::PreconditionFailed {
                    current_revision: None,
                });
            };
            if expected_revision.is_some_and(|expected| expected != current.revision) {
                return Ok(MutationResult::PreconditionFailed {
                    current_revision: Some(current.revision),
                });
            }
            transaction.execute("DELETE FROM kv_state WHERE key = ?1", params![key])?;
            Ok(MutationResult::Deleted { revision })
        }
    }
}

fn write_value(
    transaction: &Transaction<'_>,
    key: &str,
    value: &[u8],
    revision: u64,
) -> Result<(), StorageError> {
    transaction.execute(
        "INSERT INTO kv_state(key, value, revision) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, revision = excluded.revision",
        params![key, value, encode_u64(revision).as_slice()],
    )?;
    Ok(())
}

fn load_value(connection: &Connection, key: &str) -> Result<Option<ValueRecord>, StorageError> {
    let row = connection
        .query_row(
            "SELECT value, revision FROM kv_state WHERE key = ?1",
            params![key],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    row.map(|(value, revision)| {
        Ok(ValueRecord {
            value,
            revision: decode_u64("revision", &revision)?,
        })
    })
    .transpose()
}

fn load_client(
    connection: &Connection,
    client_id: ClientId,
) -> Result<Option<(u64, [u8; 32], MutationResult)>, StorageError> {
    let row = connection
        .query_row(
            "SELECT sequence, fingerprint, result FROM client_sessions WHERE client_id = ?1",
            params![client_id.0.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    row.map(|(sequence, fingerprint, result)| {
        let fingerprint = fingerprint
            .try_into()
            .map_err(|bytes: Vec<u8>| StorageError::InvalidFingerprintEncoding(bytes.len()))?;
        Ok((
            decode_u64("client sequence", &sequence)?,
            fingerprint,
            decode_mutation_result(&result).map_err(StorageError::Command)?,
        ))
    })
    .transpose()
}

fn load_state_machine_meta(connection: &Connection) -> Result<(u64, u64), StorageError> {
    let (last_applied, max_clients): (Vec<u8>, Vec<u8>) = connection.query_row(
        "SELECT last_applied, max_clients FROM state_machine_meta WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((
        decode_u64("last_applied", &last_applied)?,
        decode_u64("max_clients", &max_clients)?,
    ))
}

fn load_applied_result(
    connection: &Connection,
    index: u64,
) -> Result<Option<Option<CommandResult>>, StorageError> {
    let row = connection
        .query_row(
            "SELECT result FROM applied_results WHERE log_index = ?1",
            params![encode_u64(index).as_slice()],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()?;
    row.map(|result| {
        result
            .as_deref()
            .map(decode_command_result)
            .transpose()
            .map_err(StorageError::Command)
    })
    .transpose()
}

fn validate_hard_state_transition(current: HardState, next: HardState) -> Result<(), StorageError> {
    if next.current_term < current.current_term {
        return Err(StorageError::HardStateRegression { current, next });
    }
    if next.current_term == current.current_term {
        match (current.voted_for, next.voted_for) {
            (Some(current_vote), Some(next_vote)) if current_vote == next_vote => {}
            (None, _) => {}
            (Some(_), _) => {
                return Err(StorageError::HardStateRegression { current, next });
            }
        }
    }
    Ok(())
}

fn validate_entry_sequence(previous_index: u64, entries: &[LogEntry]) -> Result<(), StorageError> {
    let mut expected = previous_index
        .checked_add(1)
        .ok_or(StorageError::IndexExhausted)?;
    for entry in entries {
        validate_entry(entry)?;
        if entry.index != expected {
            return Err(StorageError::NonContiguousLog {
                expected,
                found: entry.index,
            });
        }
        expected = expected
            .checked_add(1)
            .ok_or(StorageError::IndexExhausted)?;
    }
    Ok(())
}

fn validate_entry(entry: &LogEntry) -> Result<(), StorageError> {
    if entry.index == 0 || entry.term == Term(0) {
        return Err(StorageError::InvalidLogEntry {
            index: entry.index,
            term: entry.term,
        });
    }
    Ok(())
}

fn load_hard_state(connection: &Connection) -> Result<HardState, StorageError> {
    let (term, voted_for): (Vec<u8>, Option<Vec<u8>>) = connection.query_row(
        "SELECT current_term, voted_for FROM hard_state WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(HardState {
        current_term: Term(decode_u64("current_term", &term)?),
        voted_for: voted_for
            .as_deref()
            .map(|bytes| decode_u64("voted_for", bytes).map(NodeId))
            .transpose()?,
    })
}

fn last_log_position(connection: &Connection) -> Result<LogPosition, StorageError> {
    let entry = connection
        .query_row(
            "SELECT log_index, term, entry_kind, payload
             FROM raft_log ORDER BY log_index DESC LIMIT 1",
            [],
            decode_entry,
        )
        .optional()?;
    if let Some(entry) = entry {
        return Ok(LogPosition {
            index: entry.index,
            term: entry.term,
        });
    }
    Ok(load_snapshot_position(connection)?.unwrap_or_default())
}

fn term_at(connection: &Connection, index: u64) -> Result<Option<Term>, StorageError> {
    if index == 0 {
        return Ok(Some(Term(0)));
    }
    let term = connection
        .query_row(
            "SELECT term FROM raft_log WHERE log_index = ?1",
            params![encode_u64(index).as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    let term = term
        .as_deref()
        .map(|bytes| decode_u64("term", bytes).map(Term))
        .transpose()?;
    if term.is_some() {
        return Ok(term);
    }
    Ok(load_snapshot_position(connection)?
        .filter(|position| position.index == index)
        .map(|position| position.term))
}

fn load_snapshot_position(connection: &Connection) -> Result<Option<LogPosition>, StorageError> {
    let row = connection
        .query_row(
            "SELECT last_included_index, last_included_term
             FROM raft_snapshot WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    row.map(|(index, term)| {
        let position = LogPosition {
            index: decode_u64("snapshot index", &index)?,
            term: Term(decode_u64("snapshot term", &term)?),
        };
        if position.index == 0 || position.term == Term(0) {
            return Err(StorageError::SnapshotEncoding);
        }
        Ok(position)
    })
    .transpose()
}

fn load_entry(connection: &Connection, index: u64) -> Result<Option<LogEntry>, StorageError> {
    connection
        .query_row(
            "SELECT log_index, term, entry_kind, payload
             FROM raft_log WHERE log_index = ?1",
            params![encode_u64(index).as_slice()],
            decode_entry,
        )
        .optional()
        .map_err(StorageError::from)
}

fn insert_entry(transaction: &Transaction<'_>, entry: &LogEntry) -> Result<(), StorageError> {
    let (kind, payload): (i64, &[u8]) = match &entry.payload {
        EntryPayload::Noop => (0, &[]),
        EntryPayload::Command(payload) => (1, payload),
    };
    transaction.execute(
        "INSERT INTO raft_log(log_index, term, entry_kind, payload)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            encode_u64(entry.index).as_slice(),
            encode_u64(entry.term.0).as_slice(),
            kind,
            payload
        ],
    )?;
    Ok(())
}

fn decode_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogEntry> {
    let index = decode_u64_sql(0, row.get_ref(0)?.as_blob()?)?;
    let term = Term(decode_u64_sql(1, row.get_ref(1)?.as_blob()?)?);
    let kind: i64 = row.get(2)?;
    let bytes: Vec<u8> = row.get(3)?;
    let payload = match kind {
        0 => EntryPayload::Noop,
        1 => EntryPayload::Command(bytes),
        value => {
            return Err(rusqlite::Error::IntegralValueOutOfRange(2, value));
        }
    };
    Ok(LogEntry {
        index,
        term,
        payload,
    })
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(field: &'static str, bytes: &[u8]) -> Result<u64, StorageError> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::InvalidIntegerEncoding {
            field,
            length: bytes.len(),
        })?;
    Ok(u64::from_be_bytes(encoded))
}

fn decode_u64_sql(column: usize, bytes: &[u8]) -> rusqlite::Result<u64> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            Box::new(error),
        )
    })?;
    Ok(u64::from_be_bytes(encoded))
}

#[derive(Debug)]
pub enum StorageError {
    Sqlite(rusqlite::Error),
    UnsupportedSchemaVersion { expected: i64, found: i64 },
    IntegrityCheckFailed(String),
    InvalidIntegerEncoding { field: &'static str, length: usize },
    InvalidFingerprintEncoding(usize),
    InvalidClientCount(i64),
    InvalidMaxClients(u64),
    MaxClientsMismatch { configured: u64, persisted: u64 },
    UnsupportedSnapshotFormat(u32),
    SnapshotDigestMismatch,
    SnapshotClusterMismatch,
    InvalidSnapshotVoters,
    SnapshotAppliedEntryMissingTerm(u64),
    OlderSnapshot { received: u64, applied: u64 },
    SnapshotConflict(u64),
    SnapshotEncoding,
    Command(CommandError),
    HardStateRegression { current: HardState, next: HardState },
    InvalidLogEntry { index: u64, term: Term },
    NonContiguousLog { expected: u64, found: u64 },
    NonContiguousApplication { expected: u64, found: u64 },
    ApplicationLogMismatch { index: u64 },
    MissingAppliedResult { index: u64 },
    AppliedBeyondLog { last_applied: u64, last_log: u64 },
    IndexExhausted,
}

impl From<rusqlite::Error> for StorageError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "SQLite storage error: {error}"),
            Self::UnsupportedSchemaVersion { expected, found } => write!(
                formatter,
                "unsupported storage schema version {found}; expected {expected}"
            ),
            Self::IntegrityCheckFailed(result) => {
                write!(formatter, "SQLite integrity check failed: {result}")
            }
            Self::InvalidIntegerEncoding { field, length } => {
                write!(formatter, "invalid {field} encoding length {length}")
            }
            Self::InvalidFingerprintEncoding(length) => {
                write!(formatter, "invalid fingerprint encoding length {length}")
            }
            Self::InvalidClientCount(count) => {
                write!(formatter, "invalid persisted client count {count}")
            }
            Self::InvalidMaxClients(value) => {
                write!(
                    formatter,
                    "maximum client count must be positive, got {value}"
                )
            }
            Self::MaxClientsMismatch {
                configured,
                persisted,
            } => write!(
                formatter,
                "configured maximum client count {configured} does not match persisted value {persisted}"
            ),
            Self::UnsupportedSnapshotFormat(version) => {
                write!(formatter, "unsupported snapshot format version {version}")
            }
            Self::SnapshotDigestMismatch => formatter.write_str("snapshot digest mismatch"),
            Self::SnapshotClusterMismatch => formatter.write_str("snapshot cluster mismatch"),
            Self::InvalidSnapshotVoters => formatter.write_str("invalid snapshot voter set"),
            Self::SnapshotAppliedEntryMissingTerm(index) => write!(
                formatter,
                "cannot snapshot applied index {index} without its log term"
            ),
            Self::OlderSnapshot { received, applied } => write!(
                formatter,
                "snapshot index {received} is older than applied index {applied}"
            ),
            Self::SnapshotConflict(index) => {
                write!(formatter, "conflicting snapshot at included index {index}")
            }
            Self::SnapshotEncoding => formatter.write_str("invalid snapshot encoding"),
            Self::Command(error) => Display::fmt(error, formatter),
            Self::HardStateRegression { current, next } => {
                write!(
                    formatter,
                    "unsafe hard-state transition {current:?} -> {next:?}"
                )
            }
            Self::InvalidLogEntry { index, term } => {
                write!(
                    formatter,
                    "invalid log entry at index {index} in term {}",
                    term.0
                )
            }
            Self::NonContiguousLog { expected, found } => {
                write!(
                    formatter,
                    "non-contiguous log: expected index {expected}, found {found}"
                )
            }
            Self::NonContiguousApplication { expected, found } => write!(
                formatter,
                "non-contiguous application: expected index {expected}, found {found}"
            ),
            Self::ApplicationLogMismatch { index } => {
                write!(
                    formatter,
                    "committed entry {index} does not match the durable log"
                )
            }
            Self::MissingAppliedResult { index } => {
                write!(
                    formatter,
                    "missing durable application result for index {index}"
                )
            }
            Self::AppliedBeyondLog {
                last_applied,
                last_log,
            } => write!(
                formatter,
                "last applied index {last_applied} exceeds durable log index {last_log}"
            ),
            Self::IndexExhausted => formatter.write_str("Raft log index exhausted"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Command(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::kv::{MutationCommand, encode_command};

    static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn entry(index: u64, term: u64, byte: u8) -> LogEntry {
        LogEntry {
            index,
            term: Term(term),
            payload: EntryPayload::Command(vec![byte]),
        }
    }

    fn command_entry(index: u64, sequence: u64, mutation: Mutation) -> LogEntry {
        let command = Command::Mutate(MutationCommand {
            client_id: ClientId([3; 16]),
            sequence,
            mutation,
        });
        LogEntry {
            index,
            term: Term(1),
            payload: EntryPayload::Command(encode_command(&command).expect("encode command")),
        }
    }

    #[test]
    fn hard_state_is_monotonic_and_vote_is_stable_within_term() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage opens");
        let voted = HardState {
            current_term: Term(3),
            voted_for: Some(NodeId(7)),
        };
        storage
            .persist_hard_state(voted)
            .expect("hard state persists");
        assert_eq!(storage.load_hard_state().expect("hard state loads"), voted);

        assert!(matches!(
            storage.persist_hard_state(HardState {
                current_term: Term(3),
                voted_for: Some(NodeId(8)),
            }),
            Err(StorageError::HardStateRegression { .. })
        ));
        assert!(matches!(
            storage.persist_hard_state(HardState {
                current_term: Term(2),
                voted_for: None,
            }),
            Err(StorageError::HardStateRegression { .. })
        ));
        assert_eq!(storage.load_hard_state().expect("hard state loads"), voted);
    }

    #[test]
    fn local_append_requires_a_contiguous_nonzero_log() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage opens");
        assert!(matches!(
            storage.append_local(&entry(2, 1, 2)),
            Err(StorageError::NonContiguousLog {
                expected: 1,
                found: 2
            })
        ));
        storage
            .append_local(&entry(1, 1, 1))
            .expect("first entry appends");
        storage
            .append_local(&entry(2, 1, 2))
            .expect("second entry appends");
        assert_eq!(
            storage.last_log_position().expect("position loads"),
            LogPosition {
                index: 2,
                term: Term(1)
            }
        );
    }

    #[test]
    fn follower_replaces_only_the_conflicting_suffix_atomically() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage opens");
        for log_entry in [entry(1, 1, 1), entry(2, 1, 2), entry(3, 2, 3)] {
            storage
                .append_local(&log_entry)
                .expect("initial entry appends");
        }

        let outcome = storage
            .append_from_leader(
                LogPosition {
                    index: 1,
                    term: Term(1),
                },
                &[entry(2, 3, 20), entry(3, 3, 30), entry(4, 3, 40)],
            )
            .expect("leader suffix applies");
        assert_eq!(outcome, AppendOutcome::Appended { last_index: 4 });
        assert_eq!(
            storage.entries_from(1, 10).expect("entries load"),
            vec![
                entry(1, 1, 1),
                entry(2, 3, 20),
                entry(3, 3, 30),
                entry(4, 3, 40)
            ]
        );
    }

    #[test]
    fn heartbeat_does_not_truncate_a_matching_suffix() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage opens");
        for log_entry in [entry(1, 1, 1), entry(2, 1, 2)] {
            storage
                .append_local(&log_entry)
                .expect("initial entry appends");
        }

        let outcome = storage
            .append_from_leader(
                LogPosition {
                    index: 1,
                    term: Term(1),
                },
                &[],
            )
            .expect("heartbeat matches");
        assert_eq!(outcome, AppendOutcome::Appended { last_index: 1 });
        assert_eq!(storage.entries_from(1, 10).expect("entries load").len(), 2);
    }

    #[test]
    fn mismatched_previous_entry_rejects_without_mutation() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage opens");
        storage
            .append_local(&entry(1, 2, 1))
            .expect("entry appends");

        let outcome = storage
            .append_from_leader(
                LogPosition {
                    index: 1,
                    term: Term(9),
                },
                &[entry(2, 9, 2)],
            )
            .expect("mismatch is a protocol result");
        assert_eq!(
            outcome,
            AppendOutcome::Rejected {
                local_term: Some(Term(2))
            }
        );
        assert_eq!(storage.entry(2).expect("entry lookup succeeds"), None);
    }

    #[test]
    fn file_backed_state_survives_clean_reopen() {
        let suffix = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "raft-kv-storage-test-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).expect("temporary directory is created");
        let database = directory.join("node.sqlite3");
        let hard_state = HardState {
            current_term: Term(11),
            voted_for: Some(NodeId(5)),
        };

        {
            let mut storage = SqliteStorage::open(&database).expect("file storage opens");
            storage
                .persist_hard_state(hard_state)
                .expect("hard state persists");
            storage
                .append_local(&entry(1, 11, 9))
                .expect("entry persists");
        }

        {
            let storage = SqliteStorage::open(&database).expect("file storage reopens");
            assert_eq!(
                storage.load_hard_state().expect("hard state loads"),
                hard_state
            );
            assert_eq!(
                storage.entry(1).expect("entry loads"),
                Some(entry(1, 11, 9))
            );
        }

        std::fs::remove_dir_all(&directory).expect("temporary directory is removed");
    }

    #[test]
    fn committed_crud_and_deduplication_apply_atomically_and_idempotently() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage");
        let entries = vec![
            LogEntry {
                index: 1,
                term: Term(1),
                payload: EntryPayload::Noop,
            },
            command_entry(
                2,
                1,
                Mutation::Create {
                    key: "alpha".into(),
                    value: vec![1, 2],
                },
            ),
            command_entry(
                3,
                1,
                Mutation::Create {
                    key: "alpha".into(),
                    value: vec![1, 2],
                },
            ),
            command_entry(
                4,
                2,
                Mutation::Update {
                    key: "alpha".into(),
                    value: vec![9],
                    expected_revision: Some(99),
                },
            ),
        ];
        for entry in &entries {
            storage.append_local(entry).expect("append log");
        }

        let results = storage.apply_committed(&entries).expect("apply batch");
        assert_eq!(results[0].result, None);
        assert_eq!(
            results[1].result,
            Some(CommandResult::Mutation(MutationResult::Created {
                revision: 2
            }))
        );
        assert_eq!(
            results[2].result,
            Some(CommandResult::Mutation(MutationResult::Duplicate(
                Box::new(MutationResult::Created { revision: 2 })
            )))
        );
        assert_eq!(
            results[3].result,
            Some(CommandResult::Mutation(
                MutationResult::PreconditionFailed {
                    current_revision: Some(2)
                }
            ))
        );
        assert_eq!(storage.last_applied().expect("last applied"), 4);
        assert_eq!(
            storage.value("alpha").expect("read value"),
            Some(ValueRecord {
                value: vec![1, 2],
                revision: 2
            })
        );
        assert_eq!(
            storage
                .apply_committed(&entries)
                .expect("idempotent replay"),
            results
        );
    }

    #[test]
    fn malformed_batch_rolls_back_state_and_last_applied() {
        let mut storage = SqliteStorage::open_in_memory().expect("storage");
        let entries = vec![
            LogEntry {
                index: 1,
                term: Term(1),
                payload: EntryPayload::Noop,
            },
            LogEntry {
                index: 2,
                term: Term(1),
                payload: EntryPayload::Command(vec![255]),
            },
        ];
        for entry in &entries {
            storage.append_local(entry).expect("append log");
        }
        assert!(matches!(
            storage.apply_committed(&entries),
            Err(StorageError::Command(_))
        ));
        assert_eq!(storage.last_applied().expect("last applied"), 0);
        assert!(
            load_applied_result(&storage.connection, 1)
                .expect("query result")
                .is_none()
        );
    }

    #[test]
    fn persisted_client_capacity_rejects_configuration_drift() {
        let suffix = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "raft-kv-capacity-test-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).expect("create temporary directory");
        let path = directory.join("capacity.sqlite3");
        drop(SqliteStorage::open_with_max_clients(&path, 7).expect("create storage"));
        assert!(matches!(
            SqliteStorage::open_with_max_clients(&path, 8),
            Err(StorageError::MaxClientsMismatch {
                configured: 8,
                persisted: 7,
            })
        ));
        SqliteStorage::open_with_max_clients(&path, 7).expect("matching capacity reopens");
        std::fs::remove_dir_all(directory).expect("remove temporary directory");
    }

    #[test]
    fn snapshot_round_trip_compacts_and_restores_state_and_sessions() {
        let mut source = SqliteStorage::open_in_memory().expect("source");
        let entries = vec![
            LogEntry {
                index: 1,
                term: Term(2),
                payload: EntryPayload::Noop,
            },
            command_entry(
                2,
                1,
                Mutation::Create {
                    key: "snapshotted".into(),
                    value: b"value".to_vec(),
                },
            ),
        ];
        for entry in &entries {
            source.append_local(entry).expect("append source log");
        }
        source.apply_committed(&entries).expect("apply source log");

        let snapshot = source
            .create_snapshot([7; 16], &[NodeId(1), NodeId(2), NodeId(3)])
            .expect("create snapshot");
        assert_eq!(
            snapshot.last_included,
            LogPosition {
                index: 2,
                term: Term(1)
            }
        );
        assert_eq!(
            source.last_log_position().expect("virtual log base"),
            snapshot.last_included
        );
        assert!(
            source
                .entries_from(1, 10)
                .expect("compacted log")
                .is_empty()
        );
        assert_eq!(
            source.current_snapshot().expect("load snapshot"),
            Some(snapshot.clone())
        );

        let mut target = SqliteStorage::open_in_memory().expect("target");
        target
            .install_snapshot([7; 16], &snapshot)
            .expect("install snapshot");
        target
            .install_snapshot([7; 16], &snapshot)
            .expect("duplicate install is idempotent");
        assert_eq!(target.last_applied().expect("restored index"), 2);
        assert_eq!(
            target.value("snapshotted").expect("restored value"),
            Some(ValueRecord {
                value: b"value".to_vec(),
                revision: 2,
            })
        );

        let retry = command_entry(
            3,
            1,
            Mutation::Create {
                key: "snapshotted".into(),
                value: b"value".to_vec(),
            },
        );
        target.append_local(&retry).expect("append after snapshot");
        assert_eq!(
            target.apply_committed(&[retry]).expect("apply retry")[0].result,
            Some(CommandResult::Mutation(MutationResult::Duplicate(
                Box::new(MutationResult::Created { revision: 2 })
            )))
        );
    }

    #[test]
    fn snapshot_validation_rejects_wrong_cluster_and_corruption() {
        let mut source = SqliteStorage::open_in_memory().expect("source");
        let noop = LogEntry {
            index: 1,
            term: Term(1),
            payload: EntryPayload::Noop,
        };
        source.append_local(&noop).expect("append");
        source.apply_committed(&[noop]).expect("apply");
        let snapshot = source
            .create_snapshot([1; 16], &[NodeId(1)])
            .expect("snapshot");
        let mut target = SqliteStorage::open_in_memory().expect("target");
        assert!(matches!(
            target.install_snapshot([2; 16], &snapshot),
            Err(StorageError::SnapshotClusterMismatch)
        ));
        let mut corrupt = snapshot;
        corrupt.payload.push(0);
        assert!(matches!(
            target.install_snapshot([1; 16], &corrupt),
            Err(StorageError::SnapshotDigestMismatch)
        ));
        assert_eq!(target.last_applied().expect("unchanged target"), 0);
    }
}
