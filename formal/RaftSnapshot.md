# M4 snapshot and compaction model

This extension model composes with the M2 Raft result by treating committed log
growth as an input. It checks atomic publication, virtual-prefix preservation,
matching-suffix retention, conflicting-suffix discard, crash/restart behavior,
bounded chunk progress, retry, and rejection with no durable mutation.

Application state is abstracted to its applied index. A published snapshot is a
record of included index, term, and that state. Transfer contents and SHA-256
are abstracted by allowing only published records to install; `RejectInvalid`
represents wrong-cluster, bad-format, bad-digest, older, and malformed inputs.
Concrete codec and digest validation are covered by Rust storage and transport
tests.
