//! Deterministic key-value state machine and canonical replicated commands.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
};

use sha2::{Digest, Sha256};

const COMMAND_VERSION: u8 = 1;
const CREATE: u8 = 1;
const UPDATE: u8 = 2;
const DELETE: u8 = 3;
const READ: u8 = 4;
const RESULT_VERSION: u8 = 1;
pub const MAX_KEY_BYTES: usize = 256;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClientId(pub [u8; 16]);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Create {
        key: String,
        value: Vec<u8>,
    },
    Update {
        key: String,
        value: Vec<u8>,
        expected_revision: Option<u64>,
    },
    Delete {
        key: String,
        expected_revision: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationCommand {
    pub client_id: ClientId,
    pub sequence: u64,
    pub mutation: Mutation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Mutate(MutationCommand),
    Read { key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationResult {
    Created { revision: u64 },
    Updated { revision: u64 },
    Deleted { revision: u64 },
    PreconditionFailed { current_revision: Option<u64> },
    Duplicate(Box<MutationResult>),
    StaleSequence { latest: u64 },
    SequenceGap { expected: u64 },
    IdentityConflict { sequence: u64 },
    ClientCapacityReached,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadResult {
    Found { value: Vec<u8>, revision: u64 },
    NotFound,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandResult {
    Mutation(MutationResult),
    Read(ReadResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueRecord {
    pub value: Vec<u8>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClientRecord {
    sequence: u64,
    fingerprint: [u8; 32],
    result: MutationResult,
}

#[derive(Clone, Debug)]
pub struct KvStateMachine {
    keys: BTreeMap<String, ValueRecord>,
    clients: BTreeMap<ClientId, ClientRecord>,
    max_clients: usize,
}

impl KvStateMachine {
    pub fn new(max_clients: usize) -> Result<Self, CommandError> {
        if max_clients == 0 {
            return Err(CommandError::InvalidClientLimit);
        }
        Ok(Self {
            keys: BTreeMap::new(),
            clients: BTreeMap::new(),
            max_clients,
        })
    }

    pub fn apply(
        &mut self,
        log_index: u64,
        command: &Command,
    ) -> Result<CommandResult, CommandError> {
        if log_index == 0 {
            return Err(CommandError::InvalidLogIndex);
        }
        validate_command(command)?;
        match command {
            Command::Read { key } => Ok(CommandResult::Read(self.read(key))),
            Command::Mutate(command) => self
                .apply_mutation(log_index, command)
                .map(CommandResult::Mutation),
        }
    }

    pub fn get(&self, key: &str) -> Option<&ValueRecord> {
        self.keys.get(key)
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    fn read(&self, key: &str) -> ReadResult {
        self.keys
            .get(key)
            .map_or(ReadResult::NotFound, |record| ReadResult::Found {
                value: record.value.clone(),
                revision: record.revision,
            })
    }

    fn apply_mutation(
        &mut self,
        log_index: u64,
        command: &MutationCommand,
    ) -> Result<MutationResult, CommandError> {
        if command.sequence == 0 {
            return Err(CommandError::InvalidSequence);
        }
        let fingerprint = fingerprint(&command.mutation)?;
        if let Some(previous) = self.clients.get(&command.client_id) {
            if command.sequence < previous.sequence {
                return Ok(MutationResult::StaleSequence {
                    latest: previous.sequence,
                });
            }
            if command.sequence == previous.sequence {
                return if fingerprint == previous.fingerprint {
                    Ok(MutationResult::Duplicate(Box::new(previous.result.clone())))
                } else {
                    Ok(MutationResult::IdentityConflict {
                        sequence: command.sequence,
                    })
                };
            }
            let expected = previous
                .sequence
                .checked_add(1)
                .ok_or(CommandError::SequenceExhausted)?;
            if command.sequence != expected {
                return Ok(MutationResult::SequenceGap { expected });
            }
        } else {
            if command.sequence != 1 {
                return Ok(MutationResult::SequenceGap { expected: 1 });
            }
            if self.clients.len() >= self.max_clients {
                return Ok(MutationResult::ClientCapacityReached);
            }
        }

        let result = self.execute_mutation(log_index, &command.mutation);
        self.clients.insert(
            command.client_id,
            ClientRecord {
                sequence: command.sequence,
                fingerprint,
                result: result.clone(),
            },
        );
        Ok(result)
    }

    fn execute_mutation(&mut self, revision: u64, mutation: &Mutation) -> MutationResult {
        match mutation {
            Mutation::Create { key, value } => {
                if let Some(current) = self.keys.get(key) {
                    return MutationResult::PreconditionFailed {
                        current_revision: Some(current.revision),
                    };
                }
                self.keys.insert(
                    key.clone(),
                    ValueRecord {
                        value: value.clone(),
                        revision,
                    },
                );
                MutationResult::Created { revision }
            }
            Mutation::Update {
                key,
                value,
                expected_revision,
            } => {
                let Some(current) = self.keys.get(key) else {
                    return MutationResult::PreconditionFailed {
                        current_revision: None,
                    };
                };
                if expected_revision.is_some_and(|expected| expected != current.revision) {
                    return MutationResult::PreconditionFailed {
                        current_revision: Some(current.revision),
                    };
                }
                self.keys.insert(
                    key.clone(),
                    ValueRecord {
                        value: value.clone(),
                        revision,
                    },
                );
                MutationResult::Updated { revision }
            }
            Mutation::Delete {
                key,
                expected_revision,
            } => {
                let Some(current) = self.keys.get(key) else {
                    return MutationResult::PreconditionFailed {
                        current_revision: None,
                    };
                };
                if expected_revision.is_some_and(|expected| expected != current.revision) {
                    return MutationResult::PreconditionFailed {
                        current_revision: Some(current.revision),
                    };
                }
                self.keys.remove(key);
                MutationResult::Deleted { revision }
            }
        }
    }
}

pub fn encode_command(command: &Command) -> Result<Vec<u8>, CommandError> {
    validate_command(command)?;
    let mut encoded = vec![COMMAND_VERSION];
    match command {
        Command::Mutate(command) => {
            encoded.push(mutation_tag(&command.mutation));
            encoded.extend_from_slice(&command.client_id.0);
            encoded.extend_from_slice(&command.sequence.to_be_bytes());
            encode_mutation_body(&mut encoded, &command.mutation)?;
        }
        Command::Read { key } => {
            encoded.push(READ);
            encode_string(&mut encoded, key)?;
        }
    }
    Ok(encoded)
}

pub fn decode_command(encoded: &[u8]) -> Result<Command, CommandError> {
    let mut decoder = Decoder::new(encoded);
    if decoder.byte()? != COMMAND_VERSION {
        return Err(CommandError::UnsupportedVersion);
    }
    let tag = decoder.byte()?;
    let command = if tag == READ {
        Command::Read {
            key: decoder.string()?,
        }
    } else {
        let client_id = ClientId(decoder.array()?);
        let sequence = decoder.u64()?;
        let key = decoder.string()?;
        let mutation = match tag {
            CREATE => Mutation::Create {
                key,
                value: decoder.bytes()?,
            },
            UPDATE => Mutation::Update {
                key,
                expected_revision: decoder.optional_revision()?,
                value: decoder.bytes()?,
            },
            DELETE => Mutation::Delete {
                key,
                expected_revision: decoder.optional_revision()?,
            },
            _ => return Err(CommandError::UnknownOperation(tag)),
        };
        Command::Mutate(MutationCommand {
            client_id,
            sequence,
            mutation,
        })
    };
    if !decoder.remaining().is_empty() {
        return Err(CommandError::TrailingBytes);
    }
    validate_command(&command)?;
    Ok(command)
}

pub fn fingerprint(mutation: &Mutation) -> Result<[u8; 32], CommandError> {
    let mut canonical = vec![COMMAND_VERSION, mutation_tag(mutation)];
    encode_mutation_body(&mut canonical, mutation)?;
    Ok(Sha256::digest(canonical).into())
}

pub fn encode_command_result(result: &CommandResult) -> Result<Vec<u8>, CommandError> {
    let mut encoded = vec![RESULT_VERSION];
    match result {
        CommandResult::Mutation(result) => encode_mutation_result_into(&mut encoded, result),
        CommandResult::Read(ReadResult::Found { value, revision }) => {
            encoded.push(20);
            encode_bytes(&mut encoded, value)?;
            encoded.extend_from_slice(&revision.to_be_bytes());
        }
        CommandResult::Read(ReadResult::NotFound) => encoded.push(21),
    }
    Ok(encoded)
}

pub fn decode_command_result(encoded: &[u8]) -> Result<CommandResult, CommandError> {
    let mut decoder = Decoder::new(encoded);
    if decoder.byte()? != RESULT_VERSION {
        return Err(CommandError::UnsupportedVersion);
    }
    let tag = decoder.byte()?;
    let result = match tag {
        1..=9 => CommandResult::Mutation(decode_mutation_result_from(tag, &mut decoder)?),
        20 => CommandResult::Read(ReadResult::Found {
            value: decoder.bytes()?,
            revision: decoder.u64()?,
        }),
        21 => CommandResult::Read(ReadResult::NotFound),
        _ => return Err(CommandError::UnknownResult(tag)),
    };
    if !decoder.remaining().is_empty() {
        return Err(CommandError::TrailingBytes);
    }
    Ok(result)
}

pub fn encode_mutation_result(result: &MutationResult) -> Result<Vec<u8>, CommandError> {
    let mut encoded = vec![RESULT_VERSION];
    encode_mutation_result_into(&mut encoded, result);
    Ok(encoded)
}

pub fn decode_mutation_result(encoded: &[u8]) -> Result<MutationResult, CommandError> {
    let mut decoder = Decoder::new(encoded);
    if decoder.byte()? != RESULT_VERSION {
        return Err(CommandError::UnsupportedVersion);
    }
    let tag = decoder.byte()?;
    let result = decode_mutation_result_from(tag, &mut decoder)?;
    if !decoder.remaining().is_empty() {
        return Err(CommandError::TrailingBytes);
    }
    Ok(result)
}

fn encode_mutation_result_into(output: &mut Vec<u8>, result: &MutationResult) {
    match result {
        MutationResult::Created { revision } => {
            output.push(1);
            output.extend_from_slice(&revision.to_be_bytes());
        }
        MutationResult::Updated { revision } => {
            output.push(2);
            output.extend_from_slice(&revision.to_be_bytes());
        }
        MutationResult::Deleted { revision } => {
            output.push(3);
            output.extend_from_slice(&revision.to_be_bytes());
        }
        MutationResult::PreconditionFailed { current_revision } => {
            output.push(4);
            encode_optional_revision(output, *current_revision);
        }
        MutationResult::Duplicate(result) => {
            output.push(5);
            encode_mutation_result_into(output, result);
        }
        MutationResult::StaleSequence { latest } => {
            output.push(6);
            output.extend_from_slice(&latest.to_be_bytes());
        }
        MutationResult::SequenceGap { expected } => {
            output.push(7);
            output.extend_from_slice(&expected.to_be_bytes());
        }
        MutationResult::IdentityConflict { sequence } => {
            output.push(8);
            output.extend_from_slice(&sequence.to_be_bytes());
        }
        MutationResult::ClientCapacityReached => output.push(9),
    }
}

fn decode_mutation_result_from(
    tag: u8,
    decoder: &mut Decoder<'_>,
) -> Result<MutationResult, CommandError> {
    match tag {
        1 => Ok(MutationResult::Created {
            revision: decoder.u64()?,
        }),
        2 => Ok(MutationResult::Updated {
            revision: decoder.u64()?,
        }),
        3 => Ok(MutationResult::Deleted {
            revision: decoder.u64()?,
        }),
        4 => Ok(MutationResult::PreconditionFailed {
            current_revision: decoder.optional_revision()?,
        }),
        5 => {
            let nested_tag = decoder.byte()?;
            Ok(MutationResult::Duplicate(Box::new(
                decode_mutation_result_from(nested_tag, decoder)?,
            )))
        }
        6 => Ok(MutationResult::StaleSequence {
            latest: decoder.u64()?,
        }),
        7 => Ok(MutationResult::SequenceGap {
            expected: decoder.u64()?,
        }),
        8 => Ok(MutationResult::IdentityConflict {
            sequence: decoder.u64()?,
        }),
        9 => Ok(MutationResult::ClientCapacityReached),
        _ => Err(CommandError::UnknownResult(tag)),
    }
}

pub fn validate_key(key: &str) -> Result<(), CommandError> {
    if key.is_empty() {
        return Err(CommandError::EmptyKey);
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(CommandError::FieldTooLarge);
    }
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(CommandError::InvalidKey);
    }
    Ok(())
}

fn validate_command(command: &Command) -> Result<(), CommandError> {
    match command {
        Command::Read { key } => validate_key(key),
        Command::Mutate(command) => {
            if command.sequence == 0 {
                return Err(CommandError::InvalidSequence);
            }
            match &command.mutation {
                Mutation::Create { key, value } | Mutation::Update { key, value, .. } => {
                    validate_key(key)?;
                    if value.len() > MAX_VALUE_BYTES {
                        return Err(CommandError::FieldTooLarge);
                    }
                    Ok(())
                }
                Mutation::Delete { key, .. } => validate_key(key),
            }
        }
    }
}

fn mutation_tag(mutation: &Mutation) -> u8 {
    match mutation {
        Mutation::Create { .. } => CREATE,
        Mutation::Update { .. } => UPDATE,
        Mutation::Delete { .. } => DELETE,
    }
}

fn encode_mutation_body(output: &mut Vec<u8>, mutation: &Mutation) -> Result<(), CommandError> {
    match mutation {
        Mutation::Create { key, value } => {
            encode_string(output, key)?;
            encode_bytes(output, value)?;
        }
        Mutation::Update {
            key,
            value,
            expected_revision,
        } => {
            encode_string(output, key)?;
            encode_optional_revision(output, *expected_revision);
            encode_bytes(output, value)?;
        }
        Mutation::Delete {
            key,
            expected_revision,
        } => {
            encode_string(output, key)?;
            encode_optional_revision(output, *expected_revision);
        }
    }
    Ok(())
}

fn encode_string(output: &mut Vec<u8>, value: &str) -> Result<(), CommandError> {
    encode_bytes(output, value.as_bytes())
}

fn encode_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), CommandError> {
    let length = u32::try_from(value.len()).map_err(|_| CommandError::FieldTooLarge)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn encode_optional_revision(output: &mut Vec<u8>, revision: Option<u64>) {
    match revision {
        Some(revision) => {
            output.push(1);
            output.extend_from_slice(&revision.to_be_bytes());
        }
        None => output.push(0),
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], CommandError> {
        if self.remaining.len() < length {
            return Err(CommandError::Truncated);
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, CommandError> {
        Ok(self.take(1)?[0])
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], CommandError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CommandError::Truncated)
    }
    fn u32(&mut self) -> Result<u32, CommandError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, CommandError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, CommandError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CommandError::FieldTooLarge)?;
        Ok(self.take(length)?.to_vec())
    }
    fn string(&mut self) -> Result<String, CommandError> {
        String::from_utf8(self.bytes()?).map_err(|_| CommandError::InvalidUtf8)
    }
    fn optional_revision(&mut self) -> Result<Option<u64>, CommandError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(CommandError::InvalidOption),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandError {
    InvalidClientLimit,
    InvalidLogIndex,
    InvalidSequence,
    SequenceExhausted,
    EmptyKey,
    InvalidKey,
    FieldTooLarge,
    UnsupportedVersion,
    UnknownOperation(u8),
    UnknownResult(u8),
    InvalidUtf8,
    InvalidOption,
    Truncated,
    TrailingBytes,
}

impl Display for CommandError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid replicated command: {self:?}")
    }
}

impl Error for CommandError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn mutation(sequence: u64, mutation: Mutation) -> Command {
        Command::Mutate(MutationCommand {
            client_id: ClientId([7; 16]),
            sequence,
            mutation,
        })
    }

    #[test]
    fn canonical_commands_round_trip() {
        let commands = [
            mutation(
                1,
                Mutation::Create {
                    key: "alpha".into(),
                    value: vec![0, 1, 255],
                },
            ),
            mutation(
                2,
                Mutation::Update {
                    key: "alpha".into(),
                    value: vec![9],
                    expected_revision: Some(42),
                },
            ),
            mutation(
                3,
                Mutation::Delete {
                    key: "alpha".into(),
                    expected_revision: None,
                },
            ),
            Command::Read {
                key: "alpha".into(),
            },
        ];
        for command in commands {
            assert_eq!(
                decode_command(&encode_command(&command).expect("encode")).expect("decode"),
                command
            );
        }
    }

    #[test]
    fn crud_preconditions_and_revisions_are_deterministic() {
        let mut state = KvStateMachine::new(8).expect("state");
        let create = mutation(
            1,
            Mutation::Create {
                key: "k".into(),
                value: b"one".to_vec(),
            },
        );
        assert_eq!(
            state.apply(3, &create).expect("apply"),
            CommandResult::Mutation(MutationResult::Created { revision: 3 })
        );
        assert_eq!(
            state.apply(4, &create).expect("dedupe"),
            CommandResult::Mutation(MutationResult::Duplicate(Box::new(
                MutationResult::Created { revision: 3 }
            )))
        );

        let wrong = mutation(
            2,
            Mutation::Update {
                key: "k".into(),
                value: b"two".to_vec(),
                expected_revision: Some(2),
            },
        );
        assert_eq!(
            state.apply(5, &wrong).expect("apply"),
            CommandResult::Mutation(MutationResult::PreconditionFailed {
                current_revision: Some(3)
            })
        );
        assert_eq!(state.get("k").expect("key").value, b"one");

        let update = mutation(
            3,
            Mutation::Update {
                key: "k".into(),
                value: b"two".to_vec(),
                expected_revision: Some(3),
            },
        );
        assert_eq!(
            state.apply(6, &update).expect("apply"),
            CommandResult::Mutation(MutationResult::Updated { revision: 6 })
        );
        let delete = mutation(
            4,
            Mutation::Delete {
                key: "k".into(),
                expected_revision: None,
            },
        );
        assert_eq!(
            state.apply(8, &delete).expect("apply"),
            CommandResult::Mutation(MutationResult::Deleted { revision: 8 })
        );
        assert_eq!(
            state
                .apply(9, &Command::Read { key: "k".into() })
                .expect("read"),
            CommandResult::Read(ReadResult::NotFound)
        );
    }

    #[test]
    fn sequence_rejections_do_not_consume_sequence() {
        let mut state = KvStateMachine::new(1).expect("state");
        let gap = mutation(
            2,
            Mutation::Delete {
                key: "k".into(),
                expected_revision: None,
            },
        );
        assert_eq!(
            state.apply(1, &gap).expect("apply"),
            CommandResult::Mutation(MutationResult::SequenceGap { expected: 1 })
        );
        let first = mutation(
            1,
            Mutation::Delete {
                key: "k".into(),
                expected_revision: None,
            },
        );
        assert_eq!(
            state.apply(2, &first).expect("apply"),
            CommandResult::Mutation(MutationResult::PreconditionFailed {
                current_revision: None
            })
        );
        let conflict = mutation(
            1,
            Mutation::Create {
                key: "k".into(),
                value: vec![],
            },
        );
        assert_eq!(
            state.apply(3, &conflict).expect("apply"),
            CommandResult::Mutation(MutationResult::IdentityConflict { sequence: 1 })
        );
    }

    #[test]
    fn fingerprints_exclude_client_identity_but_bind_request_content() {
        let first = Mutation::Update {
            key: "k".into(),
            value: vec![1],
            expected_revision: None,
        };
        let second = Mutation::Update {
            key: "k".into(),
            value: vec![2],
            expected_revision: None,
        };
        assert_eq!(
            fingerprint(&first).expect("hash"),
            fingerprint(&first).expect("hash")
        );
        assert_ne!(
            fingerprint(&first).expect("hash"),
            fingerprint(&second).expect("hash")
        );
    }

    #[test]
    fn results_round_trip_without_losing_binary_values() {
        let results = [
            CommandResult::Mutation(MutationResult::Duplicate(Box::new(
                MutationResult::PreconditionFailed {
                    current_revision: Some(8),
                },
            ))),
            CommandResult::Read(ReadResult::Found {
                value: vec![0, 255],
                revision: 9,
            }),
            CommandResult::Read(ReadResult::NotFound),
        ];
        for result in results {
            assert_eq!(
                decode_command_result(&encode_command_result(&result).expect("encode"))
                    .expect("decode"),
                result
            );
        }
    }
}
