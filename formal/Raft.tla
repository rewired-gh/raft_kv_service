------------------------------- MODULE Raft -------------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************
Static-cluster replication model for M2.

Election is a composition boundary: Elect atomically represents a completed
durable election whose voters obey the M1 RaftElection model. Log writes are
not abstracted this way. BeginAppend and AcceptAppend only stage a write;
PersistAppend makes it durable and only then emits replication messages or an
acknowledgement. Crash discards staged writes.

The network is an unordered set. A message may be delayed indefinitely,
dropped, or duplicated (duplication is observationally idempotent here).
***************************************************************************)

CONSTANTS Nodes, Values, Noop, None, MaxTerm, MaxLog, MaxMessages

ASSUME /\ Nodes # {}
       /\ None \notin Nodes
       /\ Noop \in Values
       /\ MaxTerm \in Nat \ {0}
       /\ MaxLog \in Nat \ {0}
       /\ MaxMessages \in Nat \ {0}

Roles == {"follower", "leader"}
PendingKinds == {"none", "leader", "follower"}
EntryType == [term : 1..MaxTerm, value : Values]
LogType == UNION {[1..n -> EntryType] : n \in 0..MaxLog}

LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term
Prefix(l, n) == SubSeq(l, 1, n)
IsPrefix(a, b) == /\ Len(a) <= Len(b)
                  /\ a = Prefix(b, Len(a))
UpToDate(a, b) == \/ LastTerm(a) > LastTerm(b)
                  \/ /\ LastTerm(a) = LastTerm(b)
                     /\ Len(a) >= Len(b)
MatchesPrevious(l, index, t) ==
    IF index = 0 THEN TRUE
    ELSE index <= Len(l) /\ l[index].term = t
Quorum(q) == q \subseteq Nodes /\ Cardinality(q) * 2 > Cardinality(Nodes)
Symmetry == Permutations(Nodes)
MinOf(s) == CHOOSE x \in s : \A y \in s : x <= y
MaxOf(s) == CHOOSE x \in s : \A y \in s : x >= y

AppendMessage(from, to, t, previousIndex, previousTerm, entries, leaderCommit) ==
    [kind |-> "append", from |-> from, to |-> to, term |-> t,
     previousIndex |-> previousIndex, previousTerm |-> previousTerm,
     entries |-> entries, leaderCommit |-> leaderCommit, success |-> FALSE,
     matchIndex |-> 0, requestPreviousIndex |-> previousIndex]

ResponseMessage(from, to, t, success, matchIndex, requestPreviousIndex) ==
    [kind |-> "response", from |-> from, to |-> to, term |-> t,
     previousIndex |-> 0, previousTerm |-> 0, entries |-> <<>>,
     leaderCommit |-> 0, success |-> success, matchIndex |-> matchIndex,
     requestPreviousIndex |-> requestPreviousIndex]

MessageType ==
    [kind : {"append", "response"}, from : Nodes, to : Nodes,
     term : 0..MaxTerm, previousIndex : 0..MaxLog,
     previousTerm : 0..MaxTerm, entries : LogType,
     leaderCommit : 0..MaxLog, success : BOOLEAN,
     matchIndex : 0..MaxLog, requestPreviousIndex : 0..MaxLog]

VARIABLES
    durableTerm, durableVote, voteHistory, role, log, commitIndex,
    nextIndex, matchIndex, pendingKind, pendingLog, pendingLeader,
    pendingPrevious, pendingCommit, messages, elected, committed,
    leaderLogHistory

vars ==
    <<durableTerm, durableVote, voteHistory, role, log, commitIndex,
      nextIndex, matchIndex, pendingKind, pendingLog, pendingLeader,
      pendingPrevious, pendingCommit, messages, elected, committed,
      leaderLogHistory>>

TypeOK ==
    /\ durableTerm \in [Nodes -> 0..MaxTerm]
    /\ durableVote \in [Nodes -> Nodes \cup {None}]
    /\ voteHistory \in [Nodes -> [0..MaxTerm -> Nodes \cup {None}]]
    /\ role \in [Nodes -> Roles]
    /\ log \in [Nodes -> LogType]
    /\ commitIndex \in [Nodes -> 0..MaxLog]
    /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
    /\ nextIndex \in [Nodes -> [Nodes -> 1..(MaxLog + 1)]]
    /\ matchIndex \in [Nodes -> [Nodes -> 0..MaxLog]]
    /\ pendingKind \in [Nodes -> PendingKinds]
    /\ pendingLog \in [Nodes -> LogType]
    /\ pendingLeader \in [Nodes -> Nodes \cup {None}]
    /\ pendingPrevious \in [Nodes -> 0..MaxLog]
    /\ pendingCommit \in [Nodes -> 0..MaxLog]
    /\ messages \subseteq MessageType
    /\ elected \in [0..MaxTerm -> SUBSET Nodes]
    /\ committed \in [1..MaxLog -> SUBSET EntryType]
    /\ leaderLogHistory \subseteq
         [node : Nodes, term : 1..MaxTerm, entries : LogType]

Init ==
    /\ durableTerm = [n \in Nodes |-> 0]
    /\ durableVote = [n \in Nodes |-> None]
    /\ voteHistory = [n \in Nodes |-> [t \in 0..MaxTerm |-> None]]
    /\ role = [n \in Nodes |-> "follower"]
    /\ log = [n \in Nodes |-> <<>>]
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ nextIndex = [n \in Nodes |-> [p \in Nodes |-> 1]]
    /\ matchIndex = [n \in Nodes |-> [p \in Nodes |-> 0]]
    /\ pendingKind = [n \in Nodes |-> "none"]
    /\ pendingLog = [n \in Nodes |-> <<>>]
    /\ pendingLeader = [n \in Nodes |-> None]
    /\ pendingPrevious = [n \in Nodes |-> 0]
    /\ pendingCommit = [n \in Nodes |-> 0]
    /\ messages = {}
    /\ elected = [t \in 0..MaxTerm |-> {}]
    /\ committed = [i \in 1..MaxLog |-> {}]
    /\ leaderLogHistory = {}

Elect(n, t, voters) ==
    /\ t \in 1..MaxTerm
    /\ t > durableTerm[n]
    /\ Quorum(voters)
    /\ n \in voters
    /\ \A v \in voters :
          /\ voteHistory[v][t] \in {None, n}
          /\ UpToDate(log[n], log[v])
          /\ pendingKind[v] = "none"
    /\ durableTerm' = [v \in Nodes |-> IF v \in voters THEN t ELSE durableTerm[v]]
    /\ durableVote' = [v \in Nodes |-> IF v \in voters THEN n ELSE durableVote[v]]
    /\ voteHistory' = [v \in Nodes |->
          IF v \in voters THEN [voteHistory[v] EXCEPT ![t] = n]
          ELSE voteHistory[v]]
    /\ role' = [v \in Nodes |-> IF v = n THEN "leader"
                                  ELSE IF v \in voters THEN "follower"
                                  ELSE role[v]]
    /\ nextIndex' = [nextIndex EXCEPT ![n] = [p \in Nodes |-> Len(log[n]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![n] = [p \in Nodes |-> IF p = n THEN Len(log[n]) ELSE 0]]
    /\ elected' = [elected EXCEPT ![t] = @ \cup {n}]
    /\ leaderLogHistory' = leaderLogHistory \cup
          {[node |-> n, term |-> t, entries |-> log[n]]}
    /\ UNCHANGED <<log, commitIndex, pendingKind, pendingLog, pendingLeader,
                    pendingPrevious, pendingCommit, messages, committed>>

BeginLeaderAppend(n, value) ==
    /\ role[n] = "leader"
    /\ durableTerm[n] > 0
    /\ pendingKind[n] = "none"
    /\ Len(log[n]) < MaxLog
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "leader"]
    /\ pendingLog' = [pendingLog EXCEPT ![n] =
          Append(log[n], [term |-> durableTerm[n], value |-> value])]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, nextIndex, matchIndex, pendingLeader,
                    pendingPrevious, pendingCommit, messages, elected,
                    committed, leaderLogHistory>>

PersistLeaderAppend(n) ==
    /\ pendingKind[n] = "leader"
    /\ role[n] = "leader"
    /\ log' = [log EXCEPT ![n] = pendingLog[n]]
    /\ matchIndex' = [matchIndex EXCEPT ![n][n] = Len(pendingLog[n])]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ leaderLogHistory' = leaderLogHistory \cup
          {[node |-> n, term |-> durableTerm[n], entries |-> pendingLog[n]]}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, commitIndex,
                    nextIndex, pendingLog, pendingLeader, pendingPrevious,
                    pendingCommit, messages, elected, committed>>

SendAppend(n, peer) ==
    /\ peer # n
    /\ role[n] = "leader"
    /\ pendingKind[n] = "none"
    /\ Cardinality(messages) < MaxMessages
    /\ LET previous == nextIndex[n][peer] - 1
           previousTerm == IF previous = 0 THEN 0 ELSE log[n][previous].term
           suffix == SubSeq(log[n], previous + 1, Len(log[n]))
       IN messages' = messages \cup
            {AppendMessage(n, peer, durableTerm[n], previous, previousTerm,
                           suffix, commitIndex[n])}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, nextIndex, matchIndex, pendingKind,
                    pendingLog, pendingLeader, pendingPrevious, pendingCommit,
                    elected, committed, leaderLogHistory>>

RejectAppend(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "append"
    /\ pendingKind[n] = "none"
    /\ \/ m.term < durableTerm[n]
        \/ /\ m.term = durableTerm[n]
           /\ ~MatchesPrevious(log[n], m.previousIndex, m.previousTerm)
    /\ messages' = (messages \ {m}) \cup
          {ResponseMessage(n, m.from, durableTerm[n], FALSE, 0,
                           m.previousIndex)}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, nextIndex, matchIndex, pendingKind,
                    pendingLog, pendingLeader, pendingPrevious, pendingCommit,
                    elected, committed, leaderLogHistory>>

AcceptAppend(m) ==
    LET n == m.to
        prefixOk == MatchesPrevious(log[n], m.previousIndex, m.previousTerm)
        newLog == Prefix(log[n], m.previousIndex) \o m.entries
    IN
    /\ m \in messages
    /\ m.kind = "append"
    /\ pendingKind[n] = "none"
    /\ m.term >= durableTerm[n]
    /\ prefixOk
    /\ Len(newLog) <= MaxLog
    /\ commitIndex[n] <= Len(newLog)
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "follower"]
    /\ pendingLog' = [pendingLog EXCEPT ![n] = newLog]
    /\ pendingLeader' = [pendingLeader EXCEPT ![n] = m.from]
    /\ pendingPrevious' = [pendingPrevious EXCEPT ![n] = m.previousIndex]
    /\ pendingCommit' = [pendingCommit EXCEPT ![n] =
          MinOf({m.leaderCommit, Len(newLog)})]
    /\ durableTerm' = [durableTerm EXCEPT ![n] = m.term]
    /\ durableVote' = [durableVote EXCEPT ![n] =
          IF m.term > durableTerm[n] THEN None ELSE @]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<voteHistory, log, commitIndex, nextIndex, matchIndex,
                    elected, committed, leaderLogHistory>>

PersistFollowerAppend(n) ==
    /\ pendingKind[n] = "follower"
    /\ LET leader == pendingLeader[n]
           newCommit == MaxOf({commitIndex[n], pendingCommit[n]})
       IN
       /\ leader \in Nodes
       /\ log' = [log EXCEPT ![n] = pendingLog[n]]
       /\ commitIndex' = [commitIndex EXCEPT ![n] = newCommit]
       /\ committed' = [i \in 1..MaxLog |->
             IF i > commitIndex[n] /\ i <= newCommit
             THEN committed[i] \cup {pendingLog[n][i]}
             ELSE committed[i]]
       /\ messages' = messages \cup
             {ResponseMessage(n, leader, durableTerm[n], TRUE,
                              Len(pendingLog[n]), pendingPrevious[n])}
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, nextIndex,
                    matchIndex, pendingLog, pendingLeader, pendingPrevious,
                    pendingCommit, elected, leaderLogHistory>>

HandleResponse(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "response"
    /\ m.term = durableTerm[n]
    /\ role[n] = "leader"
    /\ \/ /\ m.success
           /\ matchIndex' = [matchIndex EXCEPT ![n][m.from] =
                 MaxOf({matchIndex[n][m.from], m.matchIndex})]
           /\ nextIndex' = [nextIndex EXCEPT ![n][m.from] =
                 MaxOf({nextIndex[n][m.from], m.matchIndex + 1})]
        \/ /\ ~m.success
           /\ m.requestPreviousIndex = nextIndex[n][m.from] - 1
           /\ nextIndex' = [nextIndex EXCEPT ![n][m.from] =
                 MaxOf({1, nextIndex[n][m.from] - 1})]
           /\ UNCHANGED matchIndex
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, pendingKind, pendingLog, pendingLeader,
                    pendingPrevious, pendingCommit, elected, committed,
                    leaderLogHistory>>

IgnoreResponse(m) ==
    /\ m \in messages
    /\ m.kind = "response"
    /\ \/ m.term # durableTerm[m.to]
        \/ role[m.to] # "leader"
        \/ /\ ~m.success
           /\ m.requestPreviousIndex # nextIndex[m.to][m.from] - 1
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, nextIndex, matchIndex, pendingKind,
                    pendingLog, pendingLeader, pendingPrevious, pendingCommit,
                    elected, committed, leaderLogHistory>>

AdvanceCommit(n, index) ==
    /\ role[n] = "leader"
    /\ index \in (commitIndex[n] + 1)..Len(log[n])
    /\ log[n][index].term = durableTerm[n]
    /\ Quorum({p \in Nodes : IF p = n THEN Len(log[n]) >= index
                             ELSE matchIndex[n][p] >= index})
    /\ commitIndex' = [commitIndex EXCEPT ![n] = index]
    /\ committed' = [i \in 1..MaxLog |->
          IF i > commitIndex[n] /\ i <= index
          THEN committed[i] \cup {log[n][i]}
          ELSE committed[i]]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    nextIndex, matchIndex, pendingKind, pendingLog,
                    pendingLeader, pendingPrevious, pendingCommit, messages,
                    elected, leaderLogHistory>>

Crash(n) ==
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![n] = 0]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ nextIndex' = [nextIndex EXCEPT ![n] = [p \in Nodes |-> 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![n] = [p \in Nodes |-> 0]]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, log, pendingLog,
                    pendingLeader, pendingPrevious, pendingCommit, messages,
                    elected, committed, leaderLogHistory>>

Drop(m) ==
    /\ m \in messages
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, log,
                    commitIndex, nextIndex, matchIndex, pendingKind,
                    pendingLog, pendingLeader, pendingPrevious, pendingCommit,
                    elected, committed, leaderLogHistory>>

Next ==
    \/ \E n \in Nodes, t \in 1..MaxTerm, q \in SUBSET Nodes : Elect(n, t, q)
    \/ \E n \in Nodes, v \in Values : BeginLeaderAppend(n, v)
    \/ \E n \in Nodes : PersistLeaderAppend(n)
    \/ \E n, p \in Nodes : SendAppend(n, p)
    \/ \E m \in messages : RejectAppend(m)
    \/ \E m \in messages : AcceptAppend(m)
    \/ \E n \in Nodes : PersistFollowerAppend(n)
    \/ \E m \in messages : HandleResponse(m)
    \/ \E m \in messages : IgnoreResponse(m)
    \/ \E n \in Nodes, i \in 1..MaxLog : AdvanceCommit(n, i)
    \/ \E n \in Nodes : Crash(n)
    \/ \E m \in messages : Drop(m)

Spec == Init /\ [][Next]_vars

ElectionSafety == \A t \in 1..MaxTerm : Cardinality(elected[t]) <= 1

LogMatching ==
    \A a, b \in Nodes, i \in 1..MaxLog :
      (i <= Len(log[a]) /\ i <= Len(log[b]) /\ log[a][i].term = log[b][i].term)
      => Prefix(log[a], i) = Prefix(log[b], i)

LeaderAppendOnly ==
    \A x, y \in leaderLogHistory :
      (x.node = y.node /\ x.term = y.term)
      => (IsPrefix(x.entries, y.entries) \/ IsPrefix(y.entries, x.entries))

LeaderCompleteness ==
    \A n \in Nodes, i \in 1..MaxLog, e \in EntryType :
      (role[n] = "leader" /\ e \in committed[i] /\ e.term < durableTerm[n])
      => (i <= Len(log[n]) /\ log[n][i] = e)

StateMachineSafety ==
    \A i \in 1..MaxLog : Cardinality(committed[i]) <= 1

CommittedPrefixPreservation ==
    \A n \in Nodes, i \in 1..MaxLog :
      (commitIndex[n] >= i) => log[n][i] \in committed[i]

NoAckBeforeDurability ==
    \A m \in messages :
      (m.kind = "response" /\ m.success)
      => \E h \in leaderLogHistory :
           /\ h.node = m.to
           /\ h.term = m.term
           /\ m.matchIndex <= Len(h.entries)

=============================================================================
