# Supply-chain and release policy

Release inputs are immutable: Rust and application dependencies are locked,
base images use manifest-list digests, and GitHub Actions use full commit SHAs.
Dependabot proposes reviewed stable updates; mutable `latest` inputs are not
accepted in builds.

Every pull request runs dependency advisories, license/source policy, secret
scanning, and a production-image vulnerability scan. Fixed high or critical
image vulnerabilities, known vulnerable or yanked Rust dependencies,
unapproved licenses, unknown registries, and detected credentials fail the
gate. Unfixed OS findings are reviewed and recorded before release; exceptions
must be narrow, time-bounded, and cite an issue in the relevant tool policy.

The multi-architecture release build targets `linux/amd64` and `linux/arm64`,
emits an OCI image with BuildKit's SPDX SBOM and maximal provenance
attestations, and retains that OCI archive as CI evidence. Production runs as
numeric UID/GID 10001 with only the data volume writable. Operators mount TLS
keys read-only and should make the container root filesystem read-only.

The ordinary local gate is `make check`; `make supply-chain` runs the Rust
advisory and license/source checks. Release and scheduled workflows also run
the extended verification, fuzz, mutation, and coverage gates documented in
the verification manifest.

## Upgrade contract

Peer protocol, SQLite schema, snapshot format, trace schema, and cluster-scoped
validation limits are compatibility boundaries. A change to one requires an
ADR, a version increment, tests from the immediately prior supported format,
and a recoverable rollback or export plan. Peers reject protocol-version
mismatches before protocol processing. Storage migrations are transactional
and fail closed; downgrades across a migrated format are unsupported unless a
documented export/import path says otherwise.

Never deploy mixed binaries when their cluster-scoped key, value, append-batch,
snapshot, or retained-client bounds differ. These limits are tied to the peer
protocol release; admission is an operator-reviewed rolling-upgrade decision,
not an implicit runtime migration.
