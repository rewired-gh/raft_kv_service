# Architecture Decision Records

ADRs record decisions that constrain implementation without silently changing
the normative specification.

1. Copy `0000-template.md` to the next zero-padded number.
2. Set the status to `proposed` and describe considered alternatives.
3. Accept it in the same change that first relies on the decision.
4. Never rewrite an accepted decision's history; supersede it with a new ADR.
5. Update the specification first if a decision would alter a normative
requirement.

Accepted decisions: [0001](0001-deterministic-core.md),
[0002](0002-sqlite-storage.md), [0003](0003-peer-grpc.md),
[0004](0004-snapshot-format.md), [0005](0005-joint-membership.md),
[0006](0006-operational-hardening.md), and
[0007](0007-verification-closure.md).
