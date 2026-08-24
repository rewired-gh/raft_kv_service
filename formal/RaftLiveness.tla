--------------------------- MODULE RaftLiveness ---------------------------
EXTENDS Naturals

(***************************************************************************
Bounded recovery/liveness scenario.  The environment may partition and crash
the cluster initially.  Fair healing and fair protocol progress then require
a restarted quorum to elect, replicate, commit, and apply one command.
***************************************************************************)

VARIABLE phase
vars == <<phase>>

Init == phase = "partitioned"

Heal == /\ phase = "partitioned" /\ phase' = "healed"
RestartQuorum == /\ phase = "healed" /\ phase' = "restarted"
ElectLeader == /\ phase = "restarted" /\ phase' = "elected"
Replicate == /\ phase = "elected" /\ phase' = "replicated"
Commit == /\ phase = "replicated" /\ phase' = "committed"
Apply == /\ phase = "committed" /\ phase' = "applied"
DelayOrDrop == /\ phase = "partitioned" /\ UNCHANGED phase

Next == Heal \/ RestartQuorum \/ ElectLeader \/ Replicate \/ Commit \/ Apply
        \/ DelayOrDrop

TypeOK == phase \in {"partitioned", "healed", "restarted", "elected",
                     "replicated", "committed", "applied"}

FairSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Heal)
    /\ WF_vars(RestartQuorum)
    /\ WF_vars(ElectLeader)
    /\ WF_vars(Replicate)
    /\ WF_vars(Commit)
    /\ WF_vars(Apply)

EventuallyApplied == <> (phase = "applied")

=============================================================================
