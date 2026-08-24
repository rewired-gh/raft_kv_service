//! Exact, bounded linearizability checking for externally observed KV histories.

use std::collections::HashSet;

use crate::kv::{Command, CommandResult, KvStateMachine, MutationResult};

/// One operation interval. `result == None` represents a timeout or disconnect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub id: u64,
    pub invoked_at: u64,
    pub completed_at: Option<u64>,
    pub command: Command,
    pub result: Option<CommandResult>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckOptions {
    pub max_operations: usize,
    pub max_clients: usize,
}

impl Default for CheckOptions {
    fn default() -> Self {
        Self {
            max_operations: 24,
            max_clients: 10_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckError {
    TooManyOperations { actual: usize, maximum: usize },
    DuplicateOperationId(u64),
    CompletionBeforeInvocation(u64),
    InvalidCommand(u64),
}

/// Checks whether some legal completion and sequential ordering explains the history.
///
/// Incomplete operations are either dropped or applied with an unobserved result, as
/// required by the standard completion rule. Successful mutation revisions constrain
/// their exact Raft log positions; no-op and unobserved entries may account for gaps.
pub fn is_linearizable(
    operations: &[Operation],
    options: CheckOptions,
) -> Result<bool, CheckError> {
    if operations.len() > options.max_operations || operations.len() > 128 {
        return Err(CheckError::TooManyOperations {
            actual: operations.len(),
            maximum: options.max_operations.min(128),
        });
    }
    let mut ids = HashSet::new();
    for operation in operations {
        if !ids.insert(operation.id) {
            return Err(CheckError::DuplicateOperationId(operation.id));
        }
        if operation
            .completed_at
            .is_some_and(|completed| completed < operation.invoked_at)
        {
            return Err(CheckError::CompletionBeforeInvocation(operation.id));
        }
    }

    let state =
        KvStateMachine::new(options.max_clients).map_err(|_| CheckError::InvalidCommand(0))?;
    let remaining = if operations.len() == 128 {
        u128::MAX
    } else {
        (1_u128 << operations.len()) - 1
    };
    let predecessors = operations
        .iter()
        .map(|candidate| {
            operations
                .iter()
                .enumerate()
                .filter(|(_, other)| {
                    other
                        .completed_at
                        .is_some_and(|completed| completed < candidate.invoked_at)
                })
                .fold(0_u128, |mask, (index, _)| mask | (1_u128 << index))
        })
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    let max_observed_revision = operations
        .iter()
        .filter_map(|operation| operation.result.as_ref())
        .filter_map(observed_revision)
        .max()
        .unwrap_or(0);
    search(
        operations,
        &predecessors,
        remaining,
        0,
        state,
        max_observed_revision,
        &mut seen,
    )
}

fn search(
    operations: &[Operation],
    predecessors: &[u128],
    remaining: u128,
    last_log_index: u64,
    state: KvStateMachine,
    max_observed_revision: u64,
    seen: &mut HashSet<(u128, u64, String)>,
) -> Result<bool, CheckError> {
    if remaining == 0 {
        return Ok(true);
    }
    let memo = (remaining, last_log_index, format!("{state:?}"));
    if !seen.insert(memo) {
        return Ok(false);
    }

    for (index, operation) in operations.iter().enumerate() {
        let bit = 1_u128 << index;
        if remaining & bit == 0 || predecessors[index] & remaining != 0 {
            continue;
        }
        for apply_index in application_indices(
            operation.result.as_ref(),
            last_log_index,
            max_observed_revision,
        ) {
            let mut candidate = state.clone();
            let actual = candidate
                .apply(apply_index, &operation.command)
                .map_err(|_| CheckError::InvalidCommand(operation.id))?;
            if operation
                .result
                .as_ref()
                .is_none_or(|expected| *expected == actual)
                && search(
                    operations,
                    predecessors,
                    remaining & !bit,
                    apply_index,
                    candidate,
                    max_observed_revision,
                    seen,
                )?
            {
                return Ok(true);
            }
        }
    }

    // An incomplete operation may be omitted from the completed history.
    for (index, operation) in operations.iter().enumerate() {
        let bit = 1_u128 << index;
        if remaining & bit != 0
            && operation.result.is_none()
            && search(
                operations,
                predecessors,
                remaining & !bit,
                last_log_index,
                state.clone(),
                max_observed_revision,
                seen,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn application_indices(
    result: Option<&CommandResult>,
    last: u64,
    max_observed_revision: u64,
) -> Vec<u64> {
    let observed = match result {
        Some(CommandResult::Mutation(MutationResult::Created { revision }))
        | Some(CommandResult::Mutation(MutationResult::Updated { revision }))
        | Some(CommandResult::Mutation(MutationResult::Deleted { revision })) => Some(*revision),
        _ => None,
    };
    match observed {
        Some(revision) if revision > last => vec![revision],
        Some(_) => Vec::new(),
        None => last.checked_add(1).map_or_else(Vec::new, |next| {
            if result.is_none() && max_observed_revision >= next {
                (next..=max_observed_revision).collect()
            } else {
                vec![next]
            }
        }),
    }
}

fn observed_revision(result: &CommandResult) -> Option<u64> {
    match result {
        CommandResult::Read(crate::kv::ReadResult::Found { revision, .. })
        | CommandResult::Mutation(MutationResult::Created { revision })
        | CommandResult::Mutation(MutationResult::Updated { revision })
        | CommandResult::Mutation(MutationResult::Deleted { revision }) => Some(*revision),
        CommandResult::Mutation(MutationResult::PreconditionFailed {
            current_revision: Some(revision),
        }) => Some(*revision),
        CommandResult::Mutation(MutationResult::Duplicate(result)) => {
            observed_revision(&CommandResult::Mutation((**result).clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{ClientId, Mutation, MutationCommand, ReadResult};

    fn create(id: u64, invocation: u64, completion: Option<u64>) -> Operation {
        Operation {
            id,
            invoked_at: invocation,
            completed_at: completion,
            command: Command::Mutate(MutationCommand {
                client_id: ClientId([u8::try_from(id).expect("small id"); 16]),
                sequence: 1,
                mutation: Mutation::Create {
                    key: "key".into(),
                    value: b"value".to_vec(),
                },
            }),
            result: completion
                .map(|_| CommandResult::Mutation(MutationResult::Created { revision: id + 1 })),
        }
    }

    #[test]
    fn concurrent_operations_may_linearize_in_either_order() {
        let history = [
            Operation {
                result: Some(CommandResult::Mutation(
                    MutationResult::PreconditionFailed {
                        current_revision: Some(3),
                    },
                )),
                ..create(1, 1, Some(4))
            },
            Operation {
                result: Some(CommandResult::Mutation(MutationResult::Created {
                    revision: 3,
                })),
                ..create(2, 2, Some(3))
            },
        ];
        assert!(is_linearizable(&history, CheckOptions::default()).expect("check"));
    }

    #[test]
    fn read_after_completed_create_cannot_observe_absence() {
        let history = [
            Operation {
                result: Some(CommandResult::Mutation(MutationResult::Created {
                    revision: 2,
                })),
                ..create(1, 1, Some(2))
            },
            Operation {
                id: 2,
                invoked_at: 3,
                completed_at: Some(4),
                command: Command::Read { key: "key".into() },
                result: Some(CommandResult::Read(ReadResult::NotFound)),
            },
        ];
        assert!(!is_linearizable(&history, CheckOptions::default()).expect("check"));
    }

    #[test]
    fn incomplete_mutation_can_be_applied_or_dropped() {
        let mut pending = create(1, 1, None);
        pending.result = None;
        let found = Operation {
            id: 2,
            invoked_at: 2,
            completed_at: Some(3),
            command: Command::Read { key: "key".into() },
            result: Some(CommandResult::Read(ReadResult::Found {
                value: b"value".to_vec(),
                revision: 2,
            })),
        };
        assert!(is_linearizable(&[pending, found], CheckOptions::default()).expect("check"));
    }
}
