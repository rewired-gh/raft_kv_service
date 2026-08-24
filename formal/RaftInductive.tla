-------------------------- MODULE RaftInductive --------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
An inductive safety abstraction of the production protocol.  The detailed
bounded models remain authoritative for enabledness, message handling, and
persistence sequencing.  This module composes their durable safety boundary
so Apalache can prove initialization, one-step preservation for every action,
and implication of the named safety properties for arbitrary trace length.

Each action is named after its Rust/TLA+ counterpart.  Environmental message
loss and process failure cannot modify durable state.  Guards on Elect,
Reconcile, and Commit are the abstract obligations discharged by the detailed
models and enforced by the Rust transition core.
***************************************************************************)

CONSTANT
    \* @type: Set(Int);
    Nodes,
    \* @type: Int;
    NoNode,
    \* @type: Int;
    MaxTerm,
    \* @type: Int;
    MaxIndex

ASSUME /\ Nodes # {}
       /\ NoNode \notin Nodes
       /\ MaxTerm \in Nat \ {0}
       /\ MaxIndex \in Nat \ {0}

Roles == {"follower", "candidate", "leader"}
Phases == {"stable", "joint"}
Indexes == 1..MaxIndex
Terms == 1..MaxTerm

\* @type: (Set(Int), Set(Int)) => Bool;
Quorum(config, voters) ==
    /\ voters \subseteq config
    /\ Cardinality(voters) * 2 > Cardinality(config)

VARIABLES
    \* @type: Int -> Int;
    term,
    \* @type: Int -> (Int -> Int);
    vote,
    \* @type: Int -> Str;
    role,
    \* @type: Set({node: Int, term: Int});
    leaders,
    \* @type: Int -> (Int -> Int);
    logTerm,
    \* @type: Int -> Int;
    lastIndex,
    \* @type: Int -> Int;
    commitTerm,
    \* @type: Int -> Int;
    commitIndex,
    \* @type: Int -> Int;
    appliedIndex,
    \* @type: Int -> Int;
    snapshotIndex,
    \* @type: Int -> Int;
    durableAck,
    \* @type: Int -> Bool;
    online,
    \* @type: Str;
    phase,
    \* @type: Set(Int);
    oldConfig,
    \* @type: Set(Int);
    newConfig

vars ==
    <<term, vote, role, leaders, logTerm, lastIndex, commitTerm,
      commitIndex, appliedIndex, snapshotIndex, durableAck, online,
      phase, oldConfig, newConfig>>

TypeOK ==
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ vote \in [Nodes -> [Terms -> Nodes \cup {NoNode}]]
    /\ role \in [Nodes -> Roles]
    /\ leaders \in SUBSET [node : Nodes, term : Terms]
    /\ logTerm \in [Nodes -> [Indexes -> 0..MaxTerm]]
    /\ lastIndex \in [Nodes -> 0..MaxIndex]
    /\ commitTerm \in [Indexes -> 0..MaxTerm]
    /\ commitIndex \in [Nodes -> 0..MaxIndex]
    /\ appliedIndex \in [Nodes -> 0..MaxIndex]
    /\ snapshotIndex \in [Nodes -> 0..MaxIndex]
    /\ durableAck \in [Nodes -> 0..MaxIndex]
    /\ online \in [Nodes -> BOOLEAN]
    /\ phase \in Phases
    /\ oldConfig \in SUBSET Nodes \ {{} }
    /\ newConfig \in SUBSET Nodes \ {{} }

\* @type: ((Int -> (Int -> Int)), (Int -> Int)) => Bool;
LogShape(logs, lasts) ==
    \A n \in Nodes, i \in Indexes :
        (i <= lasts[n]) <=> (logs[n][i] > 0)

\* @type: ((Int -> (Int -> Int)), (Int -> Int)) => Bool;
LogMatchingOf(logs, lasts) ==
    \A a, b \in Nodes, i \in Indexes :
        (i <= lasts[a] /\ i <= lasts[b] /\ logs[a][i] = logs[b][i])
        => \A j \in Indexes : j <= i => logs[a][j] = logs[b][j]

ElectionSafety ==
    \A left, right \in leaders :
        left.term = right.term => left.node = right.node

LogMatching == LogMatchingOf(logTerm, lastIndex)

LeaderAppendOnly ==
    \A leader \in leaders, i \in Indexes :
        i <= lastIndex[leader.node] /\ logTerm[leader.node][i] = leader.term
        => logTerm[leader.node][i] > 0

\* @type: (Set({node: Int, term: Int}), (Int -> Int),
\*         (Int -> (Int -> Int)), (Int -> Int)) => Bool;
LeaderCompletenessOf(electedLeaders, committedTerms, logs, lasts) ==
    \A leader \in electedLeaders, i \in Indexes :
        committedTerms[i] > 0 /\ committedTerms[i] < leader.term
        => /\ i <= lasts[leader.node]
           /\ logs[leader.node][i] = committedTerms[i]

LeaderCompleteness ==
    LeaderCompletenessOf(leaders, commitTerm, logTerm, lastIndex)

StateMachineSafety ==
    \A n \in Nodes, i \in Indexes :
        i <= commitIndex[n]
        => /\ commitTerm[i] > 0
           /\ logTerm[n][i] = commitTerm[i]

CommitWitness ==
    \A i \in Indexes : commitTerm[i] > 0
        => \E n \in Nodes :
             /\ commitIndex[n] >= i
             /\ logTerm[n][i] = commitTerm[i]

CommittedPrefixPreservation ==
    \A n \in Nodes : commitIndex[n] <= lastIndex[n]

SnapshotSafety ==
    \A n \in Nodes : snapshotIndex[n] <= appliedIndex[n]
                      /\ appliedIndex[n] <= commitIndex[n]

NoAckBeforeDurability ==
    \A n \in Nodes : durableAck[n] <= lastIndex[n]

ConfigurationSafety ==
    /\ oldConfig # {}
    /\ newConfig # {}
    /\ (phase = "stable" => oldConfig = newConfig)

CoreSafety ==
    /\ ElectionSafety
    /\ LogMatching
    /\ LeaderAppendOnly
    /\ LeaderCompleteness
    /\ StateMachineSafety
    /\ CommitWitness
    /\ CommittedPrefixPreservation
    /\ SnapshotSafety
    /\ NoAckBeforeDurability
    /\ ConfigurationSafety

InductiveInvariant == /\ TypeOK /\ LogShape(logTerm, lastIndex) /\ CoreSafety

Init ==
    /\ term = [n \in Nodes |-> 0]
    /\ vote = [n \in Nodes |-> [t \in Terms |-> NoNode]]
    /\ role = [n \in Nodes |-> "follower"]
    /\ leaders = {}
    /\ logTerm = [n \in Nodes |-> [i \in Indexes |-> 0]]
    /\ lastIndex = [n \in Nodes |-> 0]
    /\ commitTerm = [i \in Indexes |-> 0]
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ appliedIndex = [n \in Nodes |-> 0]
    /\ snapshotIndex = [n \in Nodes |-> 0]
    /\ durableAck = [n \in Nodes |-> 0]
    /\ online = [n \in Nodes |-> TRUE]
    /\ phase = "stable"
    /\ oldConfig = Nodes
    /\ newConfig = Nodes

PersistTerm(n, nextTerm) ==
    /\ online[n]
    /\ nextTerm \in Terms
    /\ nextTerm > term[n]
    /\ term' = [term EXCEPT ![n] = nextTerm]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ UNCHANGED <<vote, leaders, logTerm, lastIndex, commitTerm,
                    commitIndex, appliedIndex, snapshotIndex, durableAck,
                    online, phase, oldConfig, newConfig>>

PersistVote(n, candidate) ==
    /\ online[n]
    /\ term[n] \in Terms
    /\ candidate \in Nodes
    /\ vote[n][term[n]] \in {NoNode, candidate}
    /\ vote' = [vote EXCEPT ![n][term[n]] = candidate]
    /\ UNCHANGED <<term, role, leaders, logTerm, lastIndex, commitTerm,
                    commitIndex, appliedIndex, snapshotIndex, durableAck,
                    online, phase, oldConfig, newConfig>>

Elect(candidate, electionTerm, voters) ==
    /\ online[candidate]
    /\ electionTerm = term[candidate]
    /\ electionTerm \in Terms
    /\ candidate \in oldConfig
    /\ IF phase = "stable"
          THEN Quorum(oldConfig, voters)
          ELSE Quorum(oldConfig, voters) /\ Quorum(newConfig, voters)
    /\ \A n \in voters : vote[n][electionTerm] = candidate
    /\ \A i \in Indexes :
          commitTerm[i] > 0 /\ commitTerm[i] < electionTerm
          => logTerm[candidate][i] = commitTerm[i]
    /\ \A elected \in leaders :
          elected.term = electionTerm => elected.node = candidate
    /\ leaders' = leaders \cup {[node |-> candidate, term |-> electionTerm]}
    /\ role' = [role EXCEPT ![candidate] = "leader"]
    /\ UNCHANGED <<term, vote, logTerm, lastIndex, commitTerm, commitIndex,
                    appliedIndex, snapshotIndex, durableAck, online, phase,
                    oldConfig, newConfig>>

AppendEntry(candidate, entryTerm) ==
    /\ online[candidate]
    /\ role[candidate] = "leader"
    /\ entryTerm = term[candidate]
    /\ entryTerm \in Terms
    /\ lastIndex[candidate] < MaxIndex
    /\ LET next == lastIndex[candidate] + 1
           nextLogs == [logTerm EXCEPT ![candidate][next] = entryTerm]
           nextLast == [lastIndex EXCEPT ![candidate] = next]
       IN /\ logTerm' = nextLogs
          /\ lastIndex' = nextLast
          /\ LogMatchingOf(nextLogs, nextLast)
    /\ UNCHANGED <<term, vote, role, leaders, commitTerm, commitIndex,
                    appliedIndex, snapshotIndex, durableAck, online, phase,
                    oldConfig, newConfig>>

Reconcile(n, source, prefix) ==
    /\ online[n]
    /\ online[source]
    /\ n # source
    /\ prefix \in 0..lastIndex[source]
    /\ prefix >= commitIndex[n]
    /\ LET nextLogs == [i \in Indexes |->
              IF i <= prefix THEN logTerm[source][i] ELSE 0]
           allLogs == [logTerm EXCEPT ![n] = nextLogs]
           allLast == [lastIndex EXCEPT ![n] = prefix]
       IN /\ logTerm' = allLogs
          /\ lastIndex' = allLast
          /\ LogMatchingOf(allLogs, allLast)
          /\ LeaderCompletenessOf(leaders, commitTerm, allLogs, allLast)
          /\ \A i \in Indexes :
                i <= commitIndex[n] => nextLogs[i] = commitTerm[i]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ durableAck' = [durableAck EXCEPT ![n] =
          IF @ > prefix THEN prefix ELSE @]
    /\ UNCHANGED <<term, vote, leaders, commitTerm, commitIndex,
                    appliedIndex, snapshotIndex, online, phase,
                    oldConfig, newConfig>>

Commit(n, index) ==
    /\ online[n]
    /\ role[n] = "leader"
    /\ index \in Indexes
    /\ index > commitIndex[n]
    /\ index <= lastIndex[n]
    /\ logTerm[n][index] = term[n]
    /\ \A i \in Indexes :
          i <= index => commitTerm[i] \in {0, logTerm[n][i]}
    /\ \A elected \in leaders, i \in Indexes :
          i <= index /\ logTerm[n][i] < elected.term
          => /\ i <= lastIndex[elected.node]
             /\ logTerm[elected.node][i] = logTerm[n][i]
    /\ commitTerm' = [i \in Indexes |->
          IF i <= index THEN logTerm[n][i] ELSE commitTerm[i]]
    /\ commitIndex' = [commitIndex EXCEPT ![n] = index]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    appliedIndex, snapshotIndex, durableAck, online, phase,
                    oldConfig, newConfig>>

ObserveCommit(n, source) ==
    /\ online[n]
    /\ online[source]
    /\ commitIndex[source] > commitIndex[n]
    /\ commitIndex[source] <= lastIndex[n]
    /\ \A i \in Indexes :
          i <= commitIndex[source] => logTerm[n][i] = commitTerm[i]
    /\ commitIndex' = [commitIndex EXCEPT ![n] = commitIndex[source]]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, appliedIndex, snapshotIndex, durableAck,
                    online, phase, oldConfig, newConfig>>

Apply(n) ==
    /\ online[n]
    /\ appliedIndex[n] < commitIndex[n]
    /\ appliedIndex' = [appliedIndex EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, snapshotIndex, durableAck,
                    online, phase, oldConfig, newConfig>>

PublishSnapshot(n) ==
    /\ online[n]
    /\ snapshotIndex[n] < appliedIndex[n]
    /\ snapshotIndex' = [snapshotIndex EXCEPT ![n] = appliedIndex[n]]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, appliedIndex, durableAck,
                    online, phase, oldConfig, newConfig>>

SendDurableAck(n, index) ==
    /\ online[n]
    /\ index \in 0..lastIndex[n]
    /\ durableAck' = [durableAck EXCEPT ![n] = index]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, appliedIndex, snapshotIndex,
                    online, phase, oldConfig, newConfig>>

BeginJoint(target) ==
    /\ phase = "stable"
    /\ target \in SUBSET Nodes \ {{} }
    /\ phase' = "joint"
    /\ newConfig' = target
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, appliedIndex, snapshotIndex,
                    durableAck, online, oldConfig>>

FinishJoint ==
    /\ phase = "joint"
    /\ phase' = "stable"
    /\ oldConfig' = newConfig
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, appliedIndex, snapshotIndex,
                    durableAck, online, newConfig>>

Crash(n) ==
    /\ online[n]
    /\ online' = [online EXCEPT ![n] = FALSE]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ UNCHANGED <<term, vote, leaders, logTerm, lastIndex, commitTerm,
                    commitIndex, appliedIndex, snapshotIndex, durableAck,
                    phase, oldConfig, newConfig>>

Restart(n) ==
    /\ ~online[n]
    /\ online' = [online EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<term, vote, role, leaders, logTerm, lastIndex,
                    commitTerm, commitIndex, appliedIndex, snapshotIndex,
                    durableAck, phase, oldConfig, newConfig>>

DropOrPartition == UNCHANGED vars

Next ==
    \/ \E n \in Nodes, t \in Terms : PersistTerm(n, t)
    \/ \E n, candidate \in Nodes : PersistVote(n, candidate)
    \/ \E candidate \in Nodes, t \in Terms, q \in SUBSET Nodes :
          Elect(candidate, t, q)
    \/ \E n \in Nodes, t \in Terms : AppendEntry(n, t)
    \/ \E n, source \in Nodes, prefix \in 0..MaxIndex :
          Reconcile(n, source, prefix)
    \/ \E n \in Nodes, i \in Indexes : Commit(n, i)
    \/ \E n, source \in Nodes : ObserveCommit(n, source)
    \/ \E n \in Nodes : Apply(n) \/ PublishSnapshot(n) \/ Crash(n) \/ Restart(n)
    \/ \E n \in Nodes, i \in 0..MaxIndex : SendDurableAck(n, i)
    \/ \E target \in SUBSET Nodes : BeginJoint(target)
    \/ FinishJoint
    \/ DropOrPartition

Spec == Init /\ [][Next]_vars

=============================================================================
