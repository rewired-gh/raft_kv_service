# M3 KV state-machine model

`KvStateMachine.tla` models committed CRUD application, client-session
deduplication metadata, retries, identity conflicts, logged reads, and one
crash/restart cycle. State publication is atomic: a crash cannot expose only a
subset of the KV row, session result, successful-revision set, and applied
index transition.

Fingerprints are finite command records, which encodes the specification's
injective-fingerprint abstraction. The model does not prove SHA-256 collision
resistance or SQLite itself. The fast configuration is deliberately bounded;
the implementation link is supplied independently by property-based SQLite
conformance tests and the external history checker.
