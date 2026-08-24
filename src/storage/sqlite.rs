use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    time::Duration,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};

use super::{AppendOutcome, RaftStorage};
use crate::raft::{EntryPayload, HardState, LogEntry, LogPosition, NodeId, Term};

const SCHEMA_VERSION: i64 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct SqliteStorage {
    connection: Connection,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Self::initialize(connection, true)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::initialize(Connection::open_in_memory()?, false)
    }

    fn initialize(connection: Connection, file_backed: bool) -> Result<Self, StorageError> {
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

            INSERT OR IGNORE INTO hard_state(singleton, current_term, voted_for)
            VALUES (1, X'0000000000000000', NULL);
            ",
        )?;

        let user_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match user_version {
            0 => connection.pragma_update(None, "user_version", SCHEMA_VERSION)?,
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
        let mut expected_index = 1_u64;
        for entry in self.entries_from(1, usize::MAX)? {
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
        let entry = self
            .connection
            .query_row(
                "SELECT log_index, term, entry_kind, payload
                 FROM raft_log ORDER BY log_index DESC LIMIT 1",
                [],
                decode_entry,
            )
            .optional()?;
        Ok(
            entry.map_or_else(LogPosition::default, |entry| LogPosition {
                index: entry.index,
                term: entry.term,
            }),
        )
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
    Ok(
        entry.map_or_else(LogPosition::default, |entry| LogPosition {
            index: entry.index,
            term: entry.term,
        }),
    )
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
    term.as_deref()
        .map(|bytes| decode_u64("term", bytes).map(Term))
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
    HardStateRegression { current: HardState, next: HardState },
    InvalidLogEntry { index: u64, term: Term },
    NonContiguousLog { expected: u64, found: u64 },
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
            Self::IndexExhausted => formatter.write_str("Raft log index exhausted"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn entry(index: u64, term: u64, byte: u8) -> LogEntry {
        LogEntry {
            index,
            term: Term(term),
            payload: EntryPayload::Command(vec![byte]),
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
}
