# Transaction design

Sekas uses MVCC to provide versioned reads. A Raft group can apply a batch atomically across
its shards. Transactions that use the distributed protocol coordinate several such batches
using write intents and a transaction state table. The client selects a commit path based on
the transaction's reads, write set, and current routing. See [Raft and replica design](replica.md)
for how each group commits and applies its part of the protocol.

## Versions and reads

- **Start version.** A transaction lazily requests a version from the Root Group's allocator.
  Default versioned point and range reads share this version. The distributed protocol also
  uses it as the transaction's ID. [Client `Txn`](../src/client/src/txn.rs) · [Root allocator](../src/server/src/root/mod.rs)
- **Commit version.** All writes in a committed transaction use the same version. The commit
  path determines how the version is chosen. At a given read version, a reader can see the
  latest eligible committed value or tombstone. [Point-read evaluator](../src/server/src/replica/eval/cmd_get.rs) · [Scan evaluator](../src/server/src/replica/eval/cmd_scan.rs)
- **Write intent.** Preparation stores the proposed value or deletion at the reserved
  `TXN_INTENT_VERSION` for the key. The intent carries the transaction's start version. A
  reader that encounters a relevant intent consults the transaction state to determine whether
  and at what version it can see the mutation. [Intent format](../src/api/sekas/server/v1/txn_persistent.proto) · [Intent resolution](../src/server/src/replica/eval/latch.rs)

`Txn` buffers puts and deletes on the client. Default reads use the server snapshot. Point
reads can include buffered writes by opting into `TxnReadOptions::overlay_writes`. Range and
shard scans read the server snapshot. [Read options](../src/client/src/txn.rs) · [Overlay test](../src/server/tests/test_txn.rs)

## Isolation and write conflicts

After a user snapshot read, `Txn` enables write-conflict checking during distributed intent
preparation. An ordinary write conflicts if the target key has a committed version newer than
the transaction's start version. Atomic update operations such as integer addition are
evaluated against the latest value and have their own handling. Write-only transactions can
use blind-write semantics, which order the writes by commit version. Explicit compare-and-set
conditions still apply. [Commit selection](../src/client/src/txn.rs) · [Intent evaluation](../src/server/src/replica/eval/cmd_txn.rs)

Snapshot isolation permits write skew between transactions that update disjoint keys. An
application can guard an invariant spanning several keys by making competing transactions
write a shared guard key with an appropriate condition. The isolation tests cover blind
writes, atomic updates, and conflict checking after snapshot reads. [Isolation cases](../src/server/tests/test_isolation.rs) · [Guard-key test](../src/server/tests/test_txn.rs)

## Single-group commit

The client first tries `LocalTxnWrite` when it has no allocated start version and all buffered
writes route to one group. This includes writes to distinct shards owned by the same group.
The server validates ownership. A request that hits a moving shard is ineligible for this
path and can fall back to distributed commit. [Local path selection](../src/client/src/txn.rs) · [Local evaluation](../src/server/src/replica/eval/cmd_local_txn.rs)

The client gets a candidate commit version from Root. The replica's `LocalTxnManager` raises
the candidate as needed above both the highest finite read version observed locally and the
last version it assigned. Reads at a finite version wait for pending local commits at or below
that version. The leader evaluates the batch under row latches and proposes the writes together
at the selected version. This path uses one group proposal for the mutations and skips the
transaction state record and per-key intent stages. A failed write condition discards the
evaluated batch. [Version ordering](../src/server/src/replica/local_txn.rs) · [Proposal path](../src/server/src/replica/mod.rs)

For example, a write-only batch to two shards in group 7 can produce one proposal. Both keys
become visible at the same commit version after application. If routing places one of those
keys in group 8 instead, the client prepares intents in each participant group and records
their shared commit decision in the transaction state table.

## Distributed two-phase commit

```text
Transaction record:       Running ---------------------> Committed
                             |                               |
Client and participants:    +-> prepare intents              +-> finalize intents
                                 in participant groups           in participant groups
```

1. **Begin.** The client creates a `Running` record in the sharded system transaction table,
   keyed by start version, and renews its heartbeat while committing.
2. **Prepare.** The client groups writes by participant Raft group. Each group resolves
   conflicting intents, evaluates write conditions and any required snapshot conflicts, and
   replicates the prepared intents. Responses track the result of each write entry.
3. **Decide.** After every prepare succeeds, the client obtains a commit version from Root and
   changes the transaction record to `Committed` with that version. This is the commit decision.
4. **Finalize.** The client dispatches `CommitIntent` to participant groups in the background.
   Each group replaces its matching intent with a value or tombstone at the commit version, or
   removes it for a no-op. Readers can resolve committed intents before this cleanup arrives.

The state table uses conditional updates to protect the decision when retries or other
resolvers race. On a commit error after creating a record, the client checks whether the
transaction already committed before attempting to abort and clear its intents.
[Client coordinator](../src/client/src/txn.rs) · [State table](../src/client/src/txn_table.rs) · [Intent apply](../src/server/src/replica/eval/cmd_txn.rs)

## Async commit

The client tries async commit for distributed batches that span groups and fit its size and
write-count limits. It writes an `AsyncCommitting` record with the participant keys and a
deadline concurrently with preparing their intents. Each prepared intent carries a candidate
version above its start version and the preceding value's version. After all prepares succeed,
the client records the maximum candidate as the common commit version. This overlaps transaction
record creation with prepare and avoids a separate Root version allocation after prepare.
Some prepare failures fall back to the regular protocol with a fresh start version after
resolving the first attempt. Write conflicts and failed write conditions are returned to the
caller. [Async path](../src/client/src/txn.rs) · [Candidate selection](../src/server/src/replica/eval/cmd_txn.rs)

If the coordinator disappears after preparing the writes, a resolver can query the listed keys
for matching intents and derive the same maximum candidate. When all intents exist, it can
record `Committed`. A foreground resolver can abort an incomplete prepare after the deadline.
When the MVCC GC interval is enabled, the background resolver scans for recoverable async
transactions and cleans up async intents once their outcome is known.
[Async resolution](../src/client/src/txn_table.rs) · [Background resolver](../src/server/src/node/job/async_txn_resolver.rs)

## Intent recovery on the read path

An intent is resolved against its transaction record. A `Committed` record supplies the version
that decides visibility and lets the reader help finalize the intent. An `Aborted` record lets
the reader clear the intent and continue to older versions. A `Running` record can make the
reader wait. After the heartbeat expires, the resolver can conditionally abort the transaction.
The async path consults the participant list and its deadline. [Latch resolution](../src/server/src/replica/eval/latch.rs) · [State transitions](../src/client/src/txn_table.rs)

For example, suppose a transaction starts at version 100 and writes keys in two groups with a
commit version of 120. A read at version 110 uses the older values. At version 130, one group
may already store the committed value while the other still stores an intent. The second group
uses the committed transaction record to resolve that intent at version 120, so both reads
from the same snapshot can observe the transaction. [Visibility check](../src/server/src/replica/eval/cmd_get.rs) · [Committed intent recovery test](../src/server/tests/test_txn.rs)
