---------------------------- MODULE RaftSnapshot ---------------------------
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
M4 extension model. The M2 Raft model establishes which entries may commit;
this model starts from a committed-log abstraction and checks atomic snapshot
publication, compaction, crash/restart, bounded resumable transfer, and suffix
retention. Application state is abstracted to the greatest applied index.
***************************************************************************)

CONSTANTS Nodes, MaxIndex, MaxTerm, TotalChunks, MaxRetries

ASSUME /\ Nodes # {}
       /\ MaxIndex \in Nat \ {0}
       /\ MaxTerm \in Nat \ {0}
       /\ TotalChunks \in Nat \ {0}
       /\ MaxRetries \in Nat

Symmetry == Permutations(Nodes)

SnapshotType == [index : 1..MaxIndex, term : 1..MaxTerm, state : 1..MaxIndex]
EmptyTransfer == [active |-> FALSE, index |-> 0, term |-> 0,
                  state |-> 0, offset |-> 0]

VARIABLES logTerm, lastIndex, committed, applied, appState,
          snapshotIndex, snapshotTerm, snapshotState, published,
          transfer, retries, rejected, online

vars == <<logTerm, lastIndex, committed, applied, appState,
          snapshotIndex, snapshotTerm, snapshotState, published,
          transfer, retries, rejected, online>>

TypeOK ==
    /\ logTerm \in [Nodes -> [1..MaxIndex -> 0..MaxTerm]]
    /\ lastIndex \in [Nodes -> 0..MaxIndex]
    /\ committed \in [Nodes -> 0..MaxIndex]
    /\ applied \in [Nodes -> 0..MaxIndex]
    /\ appState \in [Nodes -> 0..MaxIndex]
    /\ snapshotIndex \in [Nodes -> 0..MaxIndex]
    /\ snapshotTerm \in [Nodes -> 0..MaxTerm]
    /\ snapshotState \in [Nodes -> 0..MaxIndex]
    /\ published \subseteq SnapshotType
    /\ transfer \in [Nodes ->
          [active : BOOLEAN, index : 0..MaxIndex, term : 0..MaxTerm,
           state : 0..MaxIndex, offset : 0..TotalChunks]]
    /\ retries \in [Nodes -> 0..MaxRetries]
    /\ rejected \in [Nodes -> 0..MaxRetries]
    /\ online \in [Nodes -> BOOLEAN]

Init ==
    /\ logTerm = [n \in Nodes |-> [i \in 1..MaxIndex |-> 0]]
    /\ lastIndex = [n \in Nodes |-> 0]
    /\ committed = [n \in Nodes |-> 0]
    /\ applied = [n \in Nodes |-> 0]
    /\ appState = [n \in Nodes |-> 0]
    /\ snapshotIndex = [n \in Nodes |-> 0]
    /\ snapshotTerm = [n \in Nodes |-> 0]
    /\ snapshotState = [n \in Nodes |-> 0]
    /\ published = {}
    /\ transfer = [n \in Nodes |-> EmptyTransfer]
    /\ retries = [n \in Nodes |-> 0]
    /\ rejected = [n \in Nodes |-> 0]
    /\ online = [n \in Nodes |-> TRUE]

Append(n, term) ==
    LET next == lastIndex[n] + 1 IN
    /\ online[n]
    /\ lastIndex[n] < MaxIndex
    /\ logTerm' = [logTerm EXCEPT ![n][next] = term]
    /\ lastIndex' = [lastIndex EXCEPT ![n] = next]
    /\ UNCHANGED <<committed, applied, appState, snapshotIndex, snapshotTerm,
                    snapshotState, published, transfer, retries, rejected, online>>

Commit(n) ==
    /\ online[n]
    /\ committed[n] < lastIndex[n]
    /\ committed' = [committed EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<logTerm, lastIndex, applied, appState, snapshotIndex,
                    snapshotTerm, snapshotState, published, transfer, retries,
                    rejected, online>>

Apply(n) ==
    /\ online[n]
    /\ applied[n] < committed[n]
    /\ applied' = [applied EXCEPT ![n] = @ + 1]
    /\ appState' = [appState EXCEPT ![n] = applied[n] + 1]
    /\ UNCHANGED <<logTerm, lastIndex, committed, snapshotIndex, snapshotTerm,
                    snapshotState, published, transfer, retries, rejected, online>>

CreateSnapshot(n) ==
    LET index == applied[n]
        term == logTerm[n][index]
        image == [index |-> index, term |-> term, state |-> appState[n]]
    IN
    /\ online[n]
    /\ index > snapshotIndex[n]
    /\ term > 0
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = index]
    /\ snapshotTerm' = [snapshotTerm EXCEPT ![n] = term]
    /\ snapshotState' = [snapshotState EXCEPT ![n] = appState[n]]
    /\ published' = published \cup {image}
    /\ logTerm' = [logTerm EXCEPT ![n] =
          [i \in 1..MaxIndex |-> IF i <= index THEN 0 ELSE @[i]]]
    /\ UNCHANGED <<lastIndex, committed, applied, appState, transfer, retries,
                    rejected, online>>

BeginTransfer(n, image) ==
    /\ online[n]
    /\ image \in published
    /\ image.index >= applied[n]
    /\ transfer' = [transfer EXCEPT ![n] =
          [active |-> TRUE, index |-> image.index, term |-> image.term,
           state |-> image.state, offset |-> 0]]
    /\ retries' = [retries EXCEPT ![n] = 0]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    rejected, online>>

TransferChunk(n) ==
    /\ online[n]
    /\ transfer[n].active
    /\ transfer[n].offset < TotalChunks
    /\ transfer' = [transfer EXCEPT ![n].offset = @ + 1]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    retries, rejected, online>>

RetryChunk(n) ==
    /\ online[n]
    /\ transfer[n].active
    /\ retries[n] < MaxRetries
    /\ retries' = [retries EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    transfer, rejected, online>>

Install(n) ==
    LET image == transfer[n]
        matches == image.index <= lastIndex[n]
                   /\ logTerm[n][image.index] = image.term
    IN
    /\ online[n]
    /\ image.active
    /\ image.offset = TotalChunks
    /\ image.index >= committed[n]
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = image.index]
    /\ snapshotTerm' = [snapshotTerm EXCEPT ![n] = image.term]
    /\ snapshotState' = [snapshotState EXCEPT ![n] = image.state]
    /\ applied' = [applied EXCEPT ![n] = image.index]
    /\ committed' = [committed EXCEPT ![n] = IF @ > image.index THEN @ ELSE image.index]
    /\ appState' = [appState EXCEPT ![n] = image.state]
    /\ logTerm' = [logTerm EXCEPT ![n] =
          [i \in 1..MaxIndex |->
             IF i <= image.index \/ ~matches THEN 0 ELSE @[i]]]
    /\ lastIndex' = [lastIndex EXCEPT ![n] =
          IF matches THEN @ ELSE image.index]
    /\ transfer' = [transfer EXCEPT ![n] = EmptyTransfer]
    /\ UNCHANGED <<published, retries, rejected, online>>

RejectInvalid(n) ==
    /\ online[n]
    /\ rejected[n] < MaxRetries
    /\ rejected' = [rejected EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    transfer, retries, online>>

Crash(n) ==
    /\ online[n]
    /\ online' = [online EXCEPT ![n] = FALSE]
    /\ transfer' = [transfer EXCEPT ![n] = EmptyTransfer]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    retries, rejected>>

Restart(n) ==
    /\ ~online[n]
    /\ online' = [online EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<logTerm, lastIndex, committed, applied, appState,
                    snapshotIndex, snapshotTerm, snapshotState, published,
                    transfer, retries, rejected>>

Next ==
    \/ \E n \in Nodes, term \in 1..MaxTerm : Append(n, term)
    \/ \E n \in Nodes : Commit(n) \/ Apply(n) \/ CreateSnapshot(n)
    \/ \E n \in Nodes, image \in published : BeginTransfer(n, image)
    \/ \E n \in Nodes : TransferChunk(n) \/ RetryChunk(n) \/ Install(n)
    \/ \E n \in Nodes : RejectInvalid(n) \/ Crash(n) \/ Restart(n)

Spec == Init /\ [][Next]_vars

IndexOrder ==
    \A n \in Nodes : snapshotIndex[n] <= applied[n]
                      /\ applied[n] <= committed[n]
                      /\ committed[n] <= lastIndex[n]

ApplicationImageConsistent ==
    \A n \in Nodes : appState[n] = applied[n]
                      /\ snapshotState[n] = snapshotIndex[n]

VirtualPrefixValid ==
    \A n \in Nodes :
        /\ (snapshotIndex[n] = 0) = (snapshotTerm[n] = 0)
        /\ \A i \in 1..MaxIndex : i <= snapshotIndex[n] => logTerm[n][i] = 0

RetainedSuffixContiguous ==
    \A n \in Nodes : \A i \in 1..MaxIndex :
        snapshotIndex[n] < i /\ i <= lastIndex[n] => logTerm[n][i] > 0

PublishedOrigin ==
    \A n \in Nodes : snapshotIndex[n] = 0 \/
        [index |-> snapshotIndex[n], term |-> snapshotTerm[n],
         state |-> snapshotState[n]] \in published

TransferBounded ==
    \A n \in Nodes : transfer[n].offset <= TotalChunks

=============================================================================
