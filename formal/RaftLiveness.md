# RaftLiveness scenario

`RaftLiveness.tla` isolates the project's required recovery-progress claim. It
starts partitioned, then models healing, quorum restart, election, replication,
commit, and application. TLC checks `EventuallyApplied` under explicit weak
fairness for each progress action.

This is a bounded scenario, not a general asynchronous termination proof. The
claim requires eventual synchrony, a communicating durable quorum, and fair
execution. It makes no progress promise under permanent partition, permanent
quorum loss, unavailable durable storage, or unfair scheduling. Detailed safety
remains in the other TLC models and the Apalache inductive proof.
