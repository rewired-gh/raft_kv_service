--------------------------- MODULE RaftMembership --------------------------
EXTENDS FiniteSets, Naturals, TLC

(***************************************************************************
M5 feature model for this repository's membership protocol. It abstracts log
replication to acknowledger sets while retaining the production ordering:
stage additions, append C_old,new, commit it with separate old/new majorities,
append C_new, then commit it using the latest configuration. Operation IDs are
durable logical records. The integrated Raft model remains authoritative for
term, log-matching, and current-term commitment mechanics.
***************************************************************************)

CONSTANTS Nodes, InitialConfig, Operations, NoNode, NoOperation

ASSUME /\ Nodes # {}
       /\ InitialConfig \subseteq Nodes
       /\ InitialConfig # {}
       /\ Operations # {}
       /\ NoNode \notin Nodes
       /\ NoOperation \notin Operations

Configurations == SUBSET Nodes \ {{}}
Majority(config, voters) ==
    Cardinality(config \cap voters) * 2 > Cardinality(config)

VARIABLES phase, oldConfig, newConfig, operation, staged, caughtUp,
          acknowledgers, jointCommitted, leader, completed, witnesses

vars == <<phase, oldConfig, newConfig, operation, staged, caughtUp,
          acknowledgers, jointCommitted, leader, completed, witnesses>>

ActiveConfig == IF phase = "stable" THEN oldConfig ELSE oldConfig \cup newConfig

TypeOK ==
    /\ phase \in {"stable", "joint", "final"}
    /\ oldConfig \in Configurations
    /\ newConfig \in Configurations
    /\ operation \in Operations \cup {NoOperation}
    /\ staged \subseteq Nodes
    /\ caughtUp \subseteq staged
    /\ acknowledgers \subseteq ActiveConfig
    /\ jointCommitted \in BOOLEAN
    /\ leader \in Nodes \cup {NoNode}
    /\ completed \subseteq [operation : Operations, target : Configurations]
    /\ witnesses \subseteq
          [operation : Operations, old : Configurations,
           new : Configurations, voters : SUBSET Nodes]

Init ==
    /\ phase = "stable"
    /\ oldConfig = InitialConfig
    /\ newConfig = InitialConfig
    /\ operation = NoOperation
    /\ staged = {}
    /\ caughtUp = {}
    /\ acknowledgers = {}
    /\ jointCommitted = FALSE
    /\ leader = NoNode
    /\ completed = {}
    /\ witnesses = {}

Stage(node) ==
    /\ phase = "stable"
    /\ node \in Nodes \ oldConfig
    /\ staged' = staged \cup {node}
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, caughtUp,
                    acknowledgers, jointCommitted, leader, completed, witnesses>>

CatchUp(node) ==
    /\ node \in staged
    /\ caughtUp' = caughtUp \cup {node}
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, staged,
                    acknowledgers, jointCommitted, leader, completed, witnesses>>

ProposeJoint(target, op) ==
    /\ phase = "stable"
    /\ target \in Configurations
    /\ op \in Operations
    /\ target \ oldConfig \subseteq caughtUp
    /\ \A record \in completed : record.operation = op => record.target = target
    /\ phase' = "joint"
    /\ newConfig' = target
    /\ operation' = op
    /\ acknowledgers' = {}
    /\ jointCommitted' = FALSE
    /\ UNCHANGED <<oldConfig, staged, caughtUp, leader, completed, witnesses>>

Acknowledge(node) ==
    /\ phase \in {"joint", "final"}
    /\ node \in ActiveConfig
    /\ acknowledgers' = acknowledgers \cup {node}
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, staged, caughtUp,
                    jointCommitted, leader, completed, witnesses>>

CommitJoint ==
    /\ phase = "joint"
    /\ Majority(oldConfig, acknowledgers)
    /\ Majority(newConfig, acknowledgers)
    /\ jointCommitted' = TRUE
    /\ witnesses' = witnesses \cup
          {[operation |-> operation, old |-> oldConfig,
            new |-> newConfig, voters |-> acknowledgers]}
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, staged, caughtUp,
                    acknowledgers, leader, completed>>

ProposeFinal ==
    /\ phase = "joint"
    /\ jointCommitted
    /\ phase' = "final"
    /\ acknowledgers' = {}
    /\ UNCHANGED <<oldConfig, newConfig, operation, staged, caughtUp,
                    jointCommitted, leader, completed, witnesses>>

CommitFinal ==
    /\ phase = "final"
    /\ Majority(newConfig, acknowledgers)
    /\ phase' = "stable"
    /\ oldConfig' = newConfig
    /\ operation' = NoOperation
    /\ acknowledgers' = {}
    /\ jointCommitted' = FALSE
    /\ leader' = IF leader \in newConfig THEN leader ELSE NoNode
    /\ completed' = completed \cup
          {[operation |-> operation, target |-> newConfig]}
    /\ staged' = staged \ newConfig
    /\ caughtUp' = caughtUp \ newConfig
    /\ UNCHANGED <<newConfig, witnesses>>

Elect(node, voters) ==
    /\ node \in ActiveConfig
    /\ voters \subseteq ActiveConfig
    /\ IF phase = "stable"
          THEN Majority(oldConfig, voters)
          ELSE Majority(oldConfig, voters) /\ Majority(newConfig, voters)
    /\ leader' = node
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, staged, caughtUp,
                    acknowledgers, jointCommitted, completed, witnesses>>

LoseLeader ==
    /\ leader # NoNode
    /\ leader' = NoNode
    /\ UNCHANGED <<phase, oldConfig, newConfig, operation, staged, caughtUp,
                    acknowledgers, jointCommitted, completed, witnesses>>

Next ==
    \/ \E node \in Nodes : Stage(node) \/ CatchUp(node) \/ Acknowledge(node)
    \/ \E target \in Configurations, op \in Operations : ProposeJoint(target, op)
    \/ CommitJoint
    \/ ProposeFinal
    \/ CommitFinal
    \/ \E node \in Nodes, voters \in SUBSET Nodes : Elect(node, voters)
    \/ LoseLeader

Spec == Init /\ [][Next]_vars

ConfigurationSafety ==
    /\ oldConfig # {}
    /\ newConfig # {}
    /\ (phase = "stable" => oldConfig = newConfig)
    /\ (phase \in {"joint", "final"} => operation \in Operations)
    /\ (phase = "final" =>
          \E witness \in witnesses :
            /\ witness.operation = operation
            /\ witness.old = oldConfig
            /\ witness.new = newConfig)

JointWitnessesUseBothQuorums ==
    \A witness \in witnesses :
        /\ Majority(witness.old, witness.voters)
        /\ Majority(witness.new, witness.voters)

OperationIdsAreFunctional ==
    \A left, right \in completed :
        left.operation = right.operation => left.target = right.target

LeaderIsAuthorized == leader = NoNode \/ leader \in ActiveConfig

=============================================================================
