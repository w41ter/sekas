# Raft and replica design

Sekas replicates each group independently. A group can own several shards and has a replica
on each node assigned to it. The group leader serves requests for those shards. Raft orders
changes within the group, and each replica applies the same committed changes to its local
storage. Cross-group atomicity comes from the [transaction protocol](txn.md).

## Module boundaries

| Component | Responsibility and design goal |
| --- | --- |
| [`service`](../src/server/src/service/mod.rs) and [`node`](../src/server/src/node/mod.rs) | The RPC service receives requests. The node locates the local replica by group ID and manages its lifecycle, including startup and recovery. A node can host replicas for many groups. |
| [`replica`](../src/server/src/replica/mod.rs) | Checks whether the replica can serve the request, evaluates KV and metadata operations, and proposes the resulting mutations. Request semantics stay in `replica/eval`, where the leader has access to the current state and local concurrency controls. |
| [`raftgroup`](../src/server/src/raftgroup/mod.rs) | Owns one Raft worker per local replica. The worker drives the vendored Raft library, handles peer messages and ticks, and coordinates Raft log writes and application. The `StateMachine` trait separates replication from Sekas-specific application. |
| [`GroupStateMachine`](../src/server/src/replica/fsm/mod.rs) | Applies committed mutations, membership changes, and shard metadata changes in log order. It publishes descriptor and term updates after applying the batch. |
| [`GroupEngine`](../src/server/src/engine/group.rs) | Stores a replica's MVCC data and group metadata in RocksDB. A group-level batch lets changes across all its shards advance the applied index together. |

## From request to applied write

```text
GroupClient -> Node RPC -> local Replica -> evaluate request
                                                |
                                                v
Client reply <- proposal result <- Applier <- Raft commit
                                      |
                                      v
                              GroupStateMachine -> GroupEngine
```

For a `Put` targeting a shard in group 7, the client sends a group request carrying the group
ID and routing epoch. The node dispatches it to its local replica for group 7. The replica checks
that it is the leader, has applied an entry in its current term, and can serve the request against
its current descriptor. On a stale epoch it returns the current descriptor so the client can
refresh routing. The request path uses a metadata lock to coordinate with descriptor changes
and takes row latches for operations that need them.
[Client routing](../src/client/src/group_client.rs) · [Replica admission](../src/server/src/replica/mod.rs)

The leader evaluates the operation against its write view and builds an `EvalResult` containing
the evaluated mutations or metadata operations. It submits that result with the term captured
during admission. The Raft node checks the term before accepting the proposal. When an entry
commits, replicas apply the encoded result through `GroupStateMachine`. The leader evaluates
conditions and atomic operations when it forms the proposal.
[Evaluation](../src/server/src/replica/eval/mod.rs) · [Proposal admission](../src/server/src/raftgroup/node.rs)

The pending mutation overlay allows a later write to evaluate against earlier in-flight writes
after those writes release their row latches. The write view records dependencies on those
proposals. A dependent request waits for both its own proposal and those dependencies before
returning a successful result. The ordinary point-read evaluator reads the group engine and
resolves transaction intents through the latch manager.
[Write view](../src/server/src/replica/write_view.rs) · [Proposal path](../src/server/src/replica/mod.rs) · [Point reads](../src/server/src/replica/eval/cmd_get.rs)

The applier batches contiguous committed entries. `GroupStateMachine::finish_plug` writes the
batch of mutations and its final applied index and term together. The applier then completes the
matching proposal by index and term. For example, if a leader steps down and another entry
commits at the same index with a different term, the old waiter receives `NotLeader`.
[Applier](../src/server/src/raftgroup/applier.rs) · [State machine](../src/server/src/replica/fsm/mod.rs)

## Reads and progress

Ordinary `Get` and `Scan` go through replica admission and read an MVCC snapshot of the local
group engine. The transaction read version controls which values are visible. Leadership and
routing epoch checks happen before evaluation. [Replica request dispatch](../src/server/src/replica/mod.rs) · [Transaction reads](txn.md#versions-and-reads)

The Raft façade also exposes read barriers. `LeaseRead` waits for the local leader's captured
commit index to be applied. `ReadIndex` obtains an index through Raft's quorum-backed protocol
and waits for local application of that index. `Relaxed` returns immediately from the façade.
Raft log indexes track replication and application progress. MVCC versions order values and
transaction visibility. [Read policies](../src/server/src/raftgroup/mod.rs) · [Read-state handling](../src/server/src/raftgroup/node.rs)

## Log storage, recovery, and snapshots

Raft entries and hard state live in `raft-engine`. The group engine stores materialized data and
the applied state in RocksDB. The node shares a log writer across Raft workers so ordinary log
write tasks can be batched. The worker sends messages that depend on persistence after the log
write and reports the persisted `Ready` to Raft. Normal state-machine apply uses RocksDB writes
with its WAL disabled and relies on Raft log replay for entries beyond the flushed applied index.
[Raft storage](../src/server/src/raftgroup/storage.rs) · [Log writer](../src/server/src/raftgroup/io/log_writer.rs) · [Apply batching](../src/server/src/engine/group.rs)

On restart the node opens the replica's active RocksDB column family. It initializes Raft with
the group's flushed applied index and the membership from the stored group descriptor. The log
supplies entries after that index, which Raft applies as they become eligible for application.
Log compaction uses the flushed index as its upper bound. [Replica recovery](../src/server/src/node/mod.rs) · [Raft startup](../src/server/src/raftgroup/node.rs) · [Log compaction](../src/server/src/raftgroup/worker.rs)

A replica that needs a snapshot can download a checkpoint containing group data, its descriptor,
and the applied index and term. Installation prepares a new RocksDB column family. The worker
synchronously writes the snapshot log state and the new active column-family mapping in the
same `raft-engine` batch. The applier activates the prepared column family after that write.
Restart uses the persisted mapping to open the corresponding data. [Checkpoint](../src/server/src/replica/fsm/checkpoint.rs) · [Snapshot log state](../src/server/src/raftgroup/storage.rs) · [Activation](../src/server/src/raftgroup/node.rs)

## Membership and shard movement

The Root Group makes cluster-wide placement decisions. The leader-side group scheduler carries
out replica maintenance and migration. Membership changes are applied through Raft
`ConfChangeV2` entries, including transitions through joint consensus. The group descriptor
tracks both replica membership and shard ownership with an epoch that the client uses to detect
stale routing. [Group scheduler](../src/server/src/schedule/setup.rs) · [Membership apply](../src/server/src/replica/fsm/mod.rs) · [Group descriptor](../src/api/sekas/server/v1/metadata.proto)

Moving a shard transfers its key range between two groups. The destination pulls the data while
requests for the moving shard can be forwarded from the source. The replicas record movement
state and progress in their state machines, which lets a new leader resume the work. Related
examples include [a leader change during a move](../src/server/tests/test_move_shard.rs) and
[catch-up through snapshot transfer](../src/server/tests/test_snapshot.rs).
[Move controller](../src/server/src/node/move_shard/ctrl.rs) · [Replica move operations](../src/server/src/replica/move_shard.rs)
