# Sekas

## Overview

Sekas is a distributed transactional key-value store built for high throughput and low
latency. Every node runs the same `sekas` binary and can host replicas of multiple Raft groups.
Databases contain tables, and tables are partitioned into key-range **shards**. A **group** is
the unit of Raft replication and can own shards from several tables. Each **replica** holds
the local state of one group on a node. Shards can move between groups as the cluster rebalances.

The **Root Group** (group 0) maintains catalog and placement metadata, informs client routing,
and allocates transaction versions. The client uses routing metadata to find the group that
owns a key and sends the request to that group's leader. For a typical write, the leader
evaluates the request, Raft replicates the resulting changes, and each replica applies them to
its local MVCC storage. A write-only batch can use one Raft proposal when all keys belong to the
same group. Distributed commits use write intents and a transaction state table.

Start with [cluster architecture](docs/design.md), [Raft and replica design](docs/replica.md),
and [transaction design](docs/txn.md).

## Code map

### Client and protocols

- `src/api` defines shared protobuf contracts, request builders, and error types. The server's
  internal Raft and storage protobufs live in `src/server/proto`.
- `src/schema` defines system tables, system key layouts, and shard range semantics. Client and
  server code share it to interpret the same metadata and keyspace.
- `src/client` handles routing, retry, and transaction coordination. Start with
  [`rpc/router.rs`](src/client/src/rpc/router.rs) for shard ownership and group metadata,
  [`group_client.rs`](src/client/src/group_client.rs) for leader discovery and retry, or
  [`txn.rs`](src/client/src/txn.rs) for reads and commit path selection.

### Server

The paths below are relative to `src/server/src`.

- [`bootstrap.rs`](src/server/src/bootstrap.rs) assembles the node, Root service, and RPC server
  so the same process can serve data and host a Root replica. [`service/`](src/server/src/service)
  owns the RPC boundary, and [`transport/`](src/server/src/transport) handles discovery, address
  resolution, and construction of internal clients.
- [`node/`](src/server/src/node) owns local replica creation, recovery, removal, and request
  dispatch. [`node/move_shard/`](src/server/src/node/move_shard) coordinates data transfer and
  request forwarding between the source and destination groups.
- [`root/`](src/server/src/root) owns cluster-wide catalog, liveness, and placement decisions.
  [`schedule/`](src/server/src/schedule) runs group-level tasks on the group leader to maintain
  replica membership, migrate replicas, and remove orphan replicas.
- [`replica/`](src/server/src/replica) checks leadership and routing epochs, evaluates requests
  under local concurrency controls, and proposes changes. `replica/eval` owns request semantics.
  `replica/fsm` applies committed changes to data and group metadata.
- [`raftgroup/`](src/server/src/raftgroup) drives the Raft library for each local replica. It
  turns proposals, ticks, and peer messages into replication, log I/O, and ordered state machine
  application. This keeps the Raft driver reusable across state machine operations.
- [`engine/`](src/server/src/engine) implements RocksDB-backed group storage, MVCC, and garbage
  collection. A group-level write batch keeps changes across shards and the applied index
  together. The separate `src/engine` workspace crate currently has no storage implementation.

### Runtime, tooling, and integrations

- `src/runtime` defines Sekas's async runtime API so it can eventually have its own runtime
  implementation. Today it wraps Tokio for task execution, synchronization, timers, and
  shutdown.
- `src/rock` houses general-purpose data structures, utilities, and algorithms at the same
  level as the Rust standard library. `src/macro` provides the async test attribute.
- `src/bin` assembles the `sekas` CLI, including server startup, shell, benchmark, and checker
  commands. `src/parser` defines the shell statement syntax and parsing.
- `src/chaos` runs reproducible workloads and faults against independent node processes.
  `src/checker` checks histories for linearizability or snapshot isolation. Server integration
  tests live in `src/server/tests`. See the [chaos design](src/chaos/DESIGN.md).
- `layers/etcd` adapts the etcd API to Sekas. `tools/machine-bench` measures local machine
  primitives. `third/raft-rs` and `third/raft-engine` contain the vendored consensus and log
  storage libraries used by the server's Raft driver.

## Development

Keep the implementation concise and readable while pursuing high throughput and low latency.

- Reusable, standard-library-style abstractions can live in `sekas-rock` when they are
  independent of Sekas's storage and consensus semantics.
- Use `sekas-runtime` for async runtime APIs across Sekas. Keep direct Tokio usage inside
  `src/runtime` and expose any additional primitives through that crate.
