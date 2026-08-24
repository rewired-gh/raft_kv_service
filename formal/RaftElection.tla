---------------------------- MODULE RaftElection ----------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
M1 election model. Log freshness is abstracted by assuming every candidate's
log is equally up to date. The integrated M2 model introduces logs and the
RequestVote freshness guard. Network delivery may be delayed or dropped.

The pending variables model the Rust persistence barrier: durable term/vote
state changes in PersistPending, and a granted response is created only by
that action. Pre-Vote delivery is collapsed to nondeterministic prospective
grants because it deliberately changes no durable state.
***************************************************************************)

CONSTANTS Nodes, MaxTerm, None

ASSUME /\ Nodes # {}
       /\ MaxTerm \in Nat \ {0}
       /\ None \notin Nodes

Roles == {"follower", "preCandidate", "candidate", "leader"}
PendingKinds == {"none", "candidate", "vote", "term"}
MessageKinds == {"requestVote", "requestVoteResponse"}

Message(kind, from, to, term, grant) ==
    [kind |-> kind, from |-> from, to |-> to, term |-> term, grant |-> grant]

RequestVote(from, to, term) ==
    Message("requestVote", from, to, term, FALSE)

VoteResponse(from, to, term, grant) ==
    Message("requestVoteResponse", from, to, term, grant)

PreVote(from, to, term) == Message("preVote", from, to, term, FALSE)
PreVoteResponse(from, to, term, grant) ==
    Message("preVoteResponse", from, to, term, grant)

MessageType ==
    [kind : MessageKinds,
     from : Nodes,
     to : Nodes,
     term : 0..MaxTerm,
     grant : BOOLEAN]

Quorum(voters) == Cardinality(voters \cap Nodes) * 2 > Cardinality(Nodes)

VARIABLES
    durableTerm,
    durableVote,
    voteHistory,
    role,
    preVotes,
    votes,
    pending,
    pendingKind,
    pendingTerm,
    pendingVote,
    pendingReply,
    messages,
    elected

vars ==
    <<durableTerm, durableVote, voteHistory, role, preVotes, votes, pending, pendingKind,
      pendingTerm, pendingVote, pendingReply, messages, elected>>

TypeOK ==
    /\ durableTerm \in [Nodes -> 0..MaxTerm]
    /\ durableVote \in [Nodes -> Nodes \cup {None}]
    /\ voteHistory \in [Nodes -> [0..MaxTerm -> Nodes \cup {None}]]
    /\ role \in [Nodes -> Roles]
    /\ preVotes \in [Nodes -> SUBSET Nodes]
    /\ votes \in [Nodes -> SUBSET Nodes]
    /\ pending \in [Nodes -> BOOLEAN]
    /\ pendingKind \in [Nodes -> PendingKinds]
    /\ pendingTerm \in [Nodes -> 0..MaxTerm]
    /\ pendingVote \in [Nodes -> Nodes \cup {None}]
    /\ pendingReply \in [Nodes -> Nodes \cup {None}]
    /\ messages \subseteq MessageType
    /\ elected \in [0..MaxTerm -> SUBSET Nodes]

Init ==
    /\ durableTerm = [n \in Nodes |-> 0]
    /\ durableVote = [n \in Nodes |-> None]
    /\ voteHistory = [n \in Nodes |-> [t \in 0..MaxTerm |-> None]]
    /\ role = [n \in Nodes |-> "follower"]
    /\ preVotes = [n \in Nodes |-> {}]
    /\ votes = [n \in Nodes |-> {}]
    /\ pending = [n \in Nodes |-> FALSE]
    /\ pendingKind = [n \in Nodes |-> "none"]
    /\ pendingTerm = [n \in Nodes |-> 0]
    /\ pendingVote = [n \in Nodes |-> None]
    /\ pendingReply = [n \in Nodes |-> None]
    /\ messages = {}
    /\ elected = [t \in 0..MaxTerm |-> {}]

BeginPreVote(n) ==
    /\ ~pending[n]
    /\ role[n] \in {"follower", "candidate"}
    /\ durableTerm[n] < MaxTerm
    /\ role' = [role EXCEPT ![n] = "preCandidate"]
    /\ preVotes' = [preVotes EXCEPT ![n] = {n}]
    /\ UNCHANGED messages
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, elected>>

GrantPreVote(n, voter) ==
    /\ role[n] = "preCandidate"
    /\ voter \in Nodes
    /\ preVotes' = [preVotes EXCEPT ![n] = @ \cup {voter}]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, messages, elected>>

BeginElection(n) ==
    /\ ~pending[n]
    /\ role[n] = "preCandidate"
    /\ Quorum(preVotes[n])
    /\ durableTerm[n] < MaxTerm
    /\ pending' = [pending EXCEPT ![n] = TRUE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "candidate"]
    /\ pendingTerm' = [pendingTerm EXCEPT ![n] = durableTerm[n] + 1]
    /\ pendingVote' = [pendingVote EXCEPT ![n] = n]
    /\ pendingReply' = [pendingReply EXCEPT ![n] = None]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, messages, elected>>

BeginTransferredElection(leader, target) ==
    /\ leader # target
    /\ role[leader] = "leader"
    /\ role[target] # "leader"
    /\ ~pending[target]
    /\ durableTerm[target] < MaxTerm
    /\ pending' = [pending EXCEPT ![target] = TRUE]
    /\ pendingKind' = [pendingKind EXCEPT ![target] = "candidate"]
    /\ pendingTerm' = [pendingTerm EXCEPT ![target] = durableTerm[target] + 1]
    /\ pendingVote' = [pendingVote EXCEPT ![target] = target]
    /\ pendingReply' = [pendingReply EXCEPT ![target] = None]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes,
                    messages, elected>>

CheckQuorumStepDown(n) ==
    /\ role[n] = "leader"
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, preVotes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, messages, elected>>

BeginHigherTerm(n, t) ==
    /\ ~pending[n]
    /\ t \in (durableTerm[n] + 1)..MaxTerm
    /\ pending' = [pending EXCEPT ![n] = TRUE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "term"]
    /\ pendingTerm' = [pendingTerm EXCEPT ![n] = t]
    /\ pendingVote' = [pendingVote EXCEPT ![n] = None]
    /\ pendingReply' = [pendingReply EXCEPT ![n] = None]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, messages, elected>>

BeginGrantVote(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "requestVote"
    /\ ~pending[n]
    /\ \/ m.term > durableTerm[n]
        \/ /\ m.term = durableTerm[n]
           /\ durableVote[n] = None
    /\ pending' = [pending EXCEPT ![n] = TRUE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "vote"]
    /\ pendingTerm' = [pendingTerm EXCEPT ![n] = m.term]
    /\ pendingVote' = [pendingVote EXCEPT ![n] = m.from]
    /\ pendingReply' = [pendingReply EXCEPT ![n] = m.from]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, elected>>

RepeatVote(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "requestVote"
    /\ ~pending[n]
    /\ m.term = durableTerm[n]
    /\ durableVote[n] = m.from
    /\ messages' = (messages \ {m})
                    \cup {VoteResponse(n, m.from, durableTerm[n], TRUE)}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, elected>>

RejectVote(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "requestVote"
    /\ ~pending[n]
    /\ \/ m.term < durableTerm[n]
        \/ /\ m.term = durableTerm[n]
           /\ durableVote[n] # None
           /\ durableVote[n] # m.from
    /\ messages' = (messages \ {m})
                    \cup {VoteResponse(n, m.from, durableTerm[n], FALSE)}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, elected>>

PersistCandidate(n) ==
    LET t == pendingTerm[n] IN
    /\ pending[n]
    /\ pendingKind[n] = "candidate"
    /\ durableTerm' = [durableTerm EXCEPT ![n] = t]
    /\ durableVote' = [durableVote EXCEPT ![n] = n]
    /\ voteHistory' = [voteHistory EXCEPT ![n][t] = n]
    /\ role' = [role EXCEPT ![n] = "candidate"]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ pending' = [pending EXCEPT ![n] = FALSE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ messages' = messages
                    \cup {RequestVote(n, peer, t) : peer \in Nodes \ {n}}
    /\ UNCHANGED <<preVotes, pendingTerm, pendingVote, pendingReply, elected>>

PersistVote(n) ==
    LET t == pendingTerm[n]
        candidate == pendingVote[n]
        reply == pendingReply[n]
    IN
    /\ pending[n]
    /\ pendingKind[n] = "vote"
    /\ candidate \in Nodes
    /\ reply = candidate
    /\ durableTerm' = [durableTerm EXCEPT ![n] = t]
    /\ durableVote' = [durableVote EXCEPT ![n] = candidate]
    /\ voteHistory' = [voteHistory EXCEPT ![n][t] = candidate]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ pending' = [pending EXCEPT ![n] = FALSE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ messages' = messages \cup {VoteResponse(n, reply, t, TRUE)}
    /\ UNCHANGED <<preVotes, pendingTerm, pendingVote, pendingReply, elected>>

PersistHigherTerm(n) ==
    LET t == pendingTerm[n] IN
    /\ pending[n]
    /\ pendingKind[n] = "term"
    /\ durableTerm' = [durableTerm EXCEPT ![n] = t]
    /\ durableVote' = [durableVote EXCEPT ![n] = None]
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ pending' = [pending EXCEPT ![n] = FALSE]
    /\ pendingKind' = [pendingKind EXCEPT ![n] = "none"]
    /\ UNCHANGED <<voteHistory, preVotes, pendingTerm, pendingVote, pendingReply,
                    messages, elected>>

AcceptCurrentTermLeader(n) ==
    /\ ~pending[n]
    /\ role[n] # "follower"
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, preVotes, pending, pendingKind,
                    pendingTerm, pendingVote, pendingReply, messages, elected>>

ReceiveGrantedVote(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "requestVoteResponse"
    /\ m.grant
    /\ ~pending[n]
    /\ role[n] = "candidate"
    /\ m.term = durableTerm[n]
    /\ votes' = [votes EXCEPT ![n] = @ \cup {m.from}]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, pending, pendingKind,
                    pendingTerm, pendingVote, pendingReply, elected>>

IgnoreVoteResponse(m) ==
    LET n == m.to IN
    /\ m \in messages
    /\ m.kind = "requestVoteResponse"
    /\ \/ ~m.grant
        \/ role[n] # "candidate"
        \/ m.term # durableTerm[n]
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, elected>>

WinElection(n) ==
    LET t == durableTerm[n] IN
    /\ ~pending[n]
    /\ role[n] = "candidate"
    /\ Quorum(votes[n])
    /\ role' = [role EXCEPT ![n] = "leader"]
    /\ elected' = [elected EXCEPT ![t] = @ \cup {n}]
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, preVotes, votes, pending, pendingKind,
                    pendingTerm, pendingVote, pendingReply, messages>>

DropMessage(m) ==
    /\ m \in messages
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<durableTerm, durableVote, voteHistory, role, preVotes, votes, pending,
                    pendingKind, pendingTerm, pendingVote, pendingReply, elected>>

Next ==
    \/ \E n \in Nodes : BeginPreVote(n)
    \/ \E n, voter \in Nodes : GrantPreVote(n, voter)
    \/ \E n \in Nodes : BeginElection(n)
    \/ \E leader, target \in Nodes : BeginTransferredElection(leader, target)
    \/ \E n \in Nodes : CheckQuorumStepDown(n)
    \/ \E n \in Nodes, t \in 0..MaxTerm : BeginHigherTerm(n, t)
    \/ \E n \in Nodes : PersistCandidate(n)
    \/ \E m \in messages : BeginGrantVote(m)
    \/ \E m \in messages : RepeatVote(m)
    \/ \E m \in messages : RejectVote(m)
    \/ \E n \in Nodes : PersistVote(n)
    \/ \E n \in Nodes : PersistHigherTerm(n)
    \/ \E n \in Nodes : AcceptCurrentTermLeader(n)
    \/ \E m \in messages : ReceiveGrantedVote(m)
    \/ \E m \in messages : IgnoreVoteResponse(m)
    \/ \E n \in Nodes : WinElection(n)
    \/ \E m \in messages : DropMessage(m)

Spec == Init /\ [][Next]_vars

ElectionSafety ==
    \A t \in 0..MaxTerm : Cardinality(elected[t]) <= 1

MessagesUseDurableTermAndVote ==
    \A m \in messages :
        /\ m.kind = "requestVoteResponse"
        /\ m.grant
        => voteHistory[m.from][m.term] = m.to

=============================================================================
