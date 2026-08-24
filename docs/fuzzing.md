# End-to-end fault campaign contract

`raft-kv-fuzz` runs real service child processes, concurrent CRUD clients, a
directional network nemesis, crash/restart faults, protocol-trace validation,
and the bounded exact linearizability checker. Campaign artifacts use schema
version `2`.

Run a bounded campaign from a built workspace:

```sh
cargo build --locked --bins
./target/debug/raft-kv-fuzz \
  --seed 424242 \
  --operations 12 \
  --cluster-size 3 \
  --artifact-dir target/fuzz/424242-3
```

The artifact directory contains `campaign.json`, a directly executable
`reproduce.sh`, node configurations and SQLite files, every process-attempt
trace and log, final metrics, the complete client history, actual fault times,
and any minimized non-linearizable history.

## Exact input replay

Schema 2 stores the complete typed nemesis schedule. Every step has an absolute
offset from campaign start and is one of: publish a network fault plan, stop a
specific process, or start a specific process attempt. Replay restores the
recorded seed, operation count, cluster size, fault rules, process targets,
attempt numbers, and schedule offsets:

```sh
./target/debug/raft-kv-fuzz \
  --replay target/fuzz/424242-3/campaign.json \
  --artifact-dir target/fuzz/424242-3-replay
```

Replay rejects older or unknown campaign schemas rather than silently
reconstructing a different schedule. The schedule itself is validated before
any child starts: timestamps must be ordered and bounded, node indexes and
attempts must be valid, and all directional fault rules must remain within the
cluster and supported delay/duplication limits.

Exact replay means exact generated workload and fault inputs. OS process,
socket, and thread scheduling remain deliberately outside the deterministic
claim, so response timing and the resulting valid history may differ. Both the
original and replay must independently pass trace validation and
linearizability. A failure retains its observed history and all diagnostics;
the minimizer removes operations only while the violation remains.

## Release campaigns

Pull requests run one bounded three-node seed. Deep verification runs multiple
three- and five-node seeds, while the coverage gate runs instrumented seeds
424242 and 929292. A release publishes the exact `campaign.json` files and
coverage report hashes recorded under `verification/evidence/`.
