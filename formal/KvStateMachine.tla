-------------------------- MODULE KvStateMachine --------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
Bounded model of the deterministic, durable M3 application layer. Every
operation consumes one committed log position. A crash changes only process
availability: KV, client sessions, results, and the applied index are durable
and atomically published. Fingerprints are finite command records and are
therefore injective in this model.
***************************************************************************)

CONSTANTS Keys, Clients, Values, MaxIndex, MaxSequence, None, Any

ASSUME /\ Keys # {}
       /\ Clients # {}
       /\ Values # {}
       /\ MaxIndex \in Nat \ {0}
       /\ MaxSequence \in Nat \ {0}
       /\ None \notin Keys \cup Clients \cup Values
       /\ Any \notin Keys \cup Clients \cup Values \cup {None}

Operations == {"create", "update", "delete"}
Results == {"created", "updated", "deleted", "preconditionFailed"}
Fingerprints ==
    [op : Operations,
     key : Keys,
     value : Values \cup {None},
     expected : 0..MaxIndex \cup {Any}]

VARIABLES kv, sessions, appliedIndex, successfulRevisions, lastResult,
          online, crashCount

vars == <<kv, sessions, appliedIndex, successfulRevisions, lastResult,
          online, crashCount>>

KvType ==
    [Keys -> [present : BOOLEAN,
              value : Values \cup {None},
              revision : 0..MaxIndex]]

SessionType ==
    [Clients -> [sequence : 0..MaxSequence,
                 fingerprint : Fingerprints \cup {None},
                 result : Results \cup {None}]]

TypeOK ==
    /\ kv \in KvType
    /\ sessions \in SessionType
    /\ appliedIndex \in 0..MaxIndex
    /\ successfulRevisions \subseteq 1..MaxIndex
    /\ lastResult \in Results \cup {"notFound", "found", None}
    /\ online \in BOOLEAN
    /\ crashCount \in 0..1

Init ==
    /\ kv = [k \in Keys |-> [present |-> FALSE, value |-> None, revision |-> 0]]
    /\ sessions = [c \in Clients |->
                    [sequence |-> 0, fingerprint |-> None, result |-> None]]
    /\ appliedIndex = 0
    /\ successfulRevisions = {}
    /\ lastResult = None
    /\ online = TRUE
    /\ crashCount = 0

PreconditionHolds(fp) ==
    CASE fp.op = "create" -> ~kv[fp.key].present
      [] fp.op \in {"update", "delete"} ->
            /\ kv[fp.key].present
            /\ \/ fp.expected = Any
               \/ fp.expected = kv[fp.key].revision

SuccessResult(fp) ==
    CASE fp.op = "create" -> "created"
      [] fp.op = "update" -> "updated"
      [] fp.op = "delete" -> "deleted"

Fresh(c, fp) ==
    LET next == appliedIndex + 1
        succeeds == PreconditionHolds(fp)
        outcome == IF succeeds THEN SuccessResult(fp) ELSE "preconditionFailed"
    IN
    /\ online
    /\ appliedIndex < MaxIndex
    /\ sessions[c].sequence < MaxSequence
    /\ appliedIndex' = next
    /\ kv' =
        IF ~succeeds
        THEN kv
        ELSE IF fp.op = "delete"
             THEN [kv EXCEPT ![fp.key] =
                    [present |-> FALSE, value |-> None, revision |-> 0]]
             ELSE [kv EXCEPT ![fp.key] =
                    [present |-> TRUE, value |-> fp.value, revision |-> next]]
    /\ sessions' = [sessions EXCEPT ![c] =
          [sequence |-> @.sequence + 1,
           fingerprint |-> fp,
           result |-> outcome]]
    /\ successfulRevisions' =
          IF succeeds THEN successfulRevisions \cup {next}
          ELSE successfulRevisions
    /\ lastResult' = outcome
    /\ UNCHANGED <<online, crashCount>>

Retry(c) ==
    /\ online
    /\ appliedIndex < MaxIndex
    /\ sessions[c].sequence > 0
    /\ appliedIndex' = appliedIndex + 1
    /\ lastResult' = sessions[c].result
    /\ UNCHANGED <<kv, sessions, successfulRevisions, online, crashCount>>

IdentityConflict(c, fp) ==
    /\ online
    /\ appliedIndex < MaxIndex
    /\ sessions[c].sequence > 0
    /\ sessions[c].fingerprint # fp
    /\ appliedIndex' = appliedIndex + 1
    /\ lastResult' = sessions[c].result
    /\ UNCHANGED <<kv, sessions, successfulRevisions, online, crashCount>>

Read(k) ==
    /\ online
    /\ appliedIndex < MaxIndex
    /\ appliedIndex' = appliedIndex + 1
    /\ lastResult' = IF kv[k].present THEN "found" ELSE "notFound"
    /\ UNCHANGED <<kv, sessions, successfulRevisions, online, crashCount>>

Crash ==
    /\ online
    /\ crashCount = 0
    /\ online' = FALSE
    /\ crashCount' = 1
    /\ UNCHANGED <<kv, sessions, appliedIndex, successfulRevisions, lastResult>>

Restart ==
    /\ ~online
    /\ online' = TRUE
    /\ UNCHANGED <<kv, sessions, appliedIndex, successfulRevisions,
                    lastResult, crashCount>>

Next ==
    \/ \E c \in Clients, fp \in Fingerprints : Fresh(c, fp)
    \/ \E c \in Clients : Retry(c)
    \/ \E c \in Clients, fp \in Fingerprints : IdentityConflict(c, fp)
    \/ \E k \in Keys : Read(k)
    \/ Crash
    \/ Restart

Spec == Init /\ [][Next]_vars

AbsentStateIsCanonical ==
    \A k \in Keys : ~kv[k].present =>
        /\ kv[k].value = None
        /\ kv[k].revision = 0

PresentRevisionIsSuccessful ==
    \A k \in Keys : kv[k].present =>
        /\ kv[k].revision \in successfulRevisions
        /\ kv[k].revision <= appliedIndex

LiveKeyRevisionsAreUnique ==
    \A left, right \in Keys :
        /\ kv[left].present
        /\ kv[right].present
        /\ kv[left].revision = kv[right].revision
        => left = right

SessionStateIsComplete ==
    \A c \in Clients :
        \/ /\ sessions[c].sequence = 0
           /\ sessions[c].fingerprint = None
           /\ sessions[c].result = None
        \/ /\ sessions[c].sequence > 0
           /\ sessions[c].fingerprint \in Fingerprints
           /\ sessions[c].result \in Results

DurableStateSurvivesCrash ==
    ~online => crashCount = 1

=============================================================================
