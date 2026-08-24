use proptest::prelude::*;
use raft_kv_service::{
    kv::{
        ClientId, Command, KvStateMachine, Mutation, MutationCommand, decode_command,
        encode_command,
    },
    raft::{EntryPayload, LogEntry, NodeId, Term},
    storage::{RaftStorage, SnapshotStorage, SqliteStorage, StateMachineStorage},
};

fn command_strategy() -> impl Strategy<Value = Command> {
    (
        any::<[u8; 16]>(),
        1_u64..=u64::MAX,
        "[A-Za-z0-9._~-]{1,16}",
        prop::collection::vec(any::<u8>(), 0..64),
        prop::option::of(1_u64..=u64::MAX),
        0_u8..4,
    )
        .prop_map(|(client, sequence, key, value, revision, operation)| {
            if operation == 0 {
                Command::Read { key }
            } else {
                let mutation = match operation {
                    1 => Mutation::Create { key, value },
                    2 => Mutation::Update {
                        key,
                        value,
                        expected_revision: revision,
                    },
                    _ => Mutation::Delete {
                        key,
                        expected_revision: revision,
                    },
                };
                Command::Mutate(MutationCommand {
                    client_id: ClientId(client),
                    sequence,
                    mutation,
                })
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn canonical_command_encoding_round_trips(command in command_strategy()) {
        let encoded = encode_command(&command).expect("generated command is valid");
        prop_assert_eq!(decode_command(&encoded).expect("canonical encoding decodes"), command);
    }

    #[test]
    fn command_application_is_deterministic(commands in prop::collection::vec(command_strategy(), 0..40)) {
        let mut left = KvStateMachine::new(10_000).expect("state machine");
        let mut right = left.clone();
        for (offset, command) in commands.iter().enumerate() {
            let index = u64::try_from(offset + 1).expect("bounded index");
            prop_assert_eq!(left.apply(index, command), right.apply(index, command));
        }
    }

    #[test]
    fn sqlite_application_conforms_to_memory_model(
        actions in prop::collection::vec(
            (
                "[A-Za-z0-9._~-]{1,8}",
                prop::collection::vec(any::<u8>(), 0..32),
                prop::option::of(1_u64..100),
                0_u8..3,
            ),
            0..30,
        ),
    ) {
        let mut model = KvStateMachine::new(10_000).expect("state machine");
        let mut storage = SqliteStorage::open_in_memory().expect("storage");
        let noop = LogEntry {
            index: 1,
            term: Term(1),
            payload: EntryPayload::Noop,
        };
        storage.append_local(&noop).expect("append noop");
        storage.apply_committed(&[noop]).expect("apply noop");
        let mut last_command = None;
        for (offset, (key, value, expected_revision, kind)) in actions.iter().enumerate() {
            let index = u64::try_from(offset + 2).expect("bounded index");
            let mutation = match kind {
                0 => Mutation::Create { key: key.clone(), value: value.clone() },
                1 => Mutation::Update {
                    key: key.clone(),
                    value: value.clone(),
                    expected_revision: *expected_revision,
                },
                _ => Mutation::Delete {
                    key: key.clone(),
                    expected_revision: *expected_revision,
                },
            };
            let command = Command::Mutate(MutationCommand {
                client_id: ClientId(u128::from(index).to_be_bytes()),
                sequence: 1,
                mutation,
            });
            last_command = Some(command.clone());
            let expected = model.apply(index, &command).expect("model application");
            let entry = LogEntry {
                index,
                term: Term(1),
                payload: EntryPayload::Command(encode_command(&command).expect("encode")),
            };
            storage.append_local(&entry).expect("append");
            let applied = storage.apply_committed(&[entry]).expect("apply");
            prop_assert_eq!(applied[0].result.as_ref(), Some(&expected));
            prop_assert_eq!(storage.value(key).expect("stored value"), model.get(key).cloned());
        }

        let snapshot = storage
            .create_snapshot([3; 16], &[NodeId(1)])
            .expect("create generated snapshot");
        let mut restored = SqliteStorage::open_in_memory().expect("restored storage");
        restored
            .install_snapshot([3; 16], &snapshot)
            .expect("install generated snapshot");
        for (key, ..) in &actions {
            prop_assert_eq!(restored.value(key).expect("restored value"), model.get(key).cloned());
        }
        if let Some(command) = last_command {
            let index = u64::try_from(actions.len() + 2).expect("bounded retry index");
            let retry = LogEntry {
                index,
                term: Term(1),
                payload: EntryPayload::Command(encode_command(&command).expect("encode retry")),
            };
            storage.append_local(&retry).expect("append source retry");
            restored.append_local(&retry).expect("append restored retry");
            prop_assert_eq!(
                storage
                    .apply_committed(std::slice::from_ref(&retry))
                    .expect("source retry"),
                restored.apply_committed(&[retry]).expect("restored retry"),
            );
        }
    }
}
