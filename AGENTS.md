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

The code map in [index](docs/index.md).

## Development

Keep the implementation concise and readable while pursuing high throughput and low latency.

- Reusable, standard-library-style abstractions can live in `sekas-rock` when they are
  independent of Sekas's storage and consensus semantics.
- Use `sekas-runtime` for async runtime APIs across Sekas. Keep direct Tokio usage inside
  `src/runtime` and expose any additional primitives through that crate.

### Design for maintainability

When a change introduces a rule, state, or interface, compare the proposed design
with the simplest implementation that meets the current requirements. Use one
plausible follow-up change grounded in current requirements, existing behavior,
or repository history. Identify which decision would change, what a maintainer
would need to infer, and what behavior would need to be verified again. Check
for change amplification by tracing where that decision would force coordinated
edits. Prefer the design that reduces this work after accounting for any new
state or guarantees it introduces.

Keep decisions that can change independently separate. Define each shared rule
in one place, and derive dependent values from their authoritative state where
practical. Prefer interfaces or state representations that enforce invariants by
preventing invalid combinations. Enforce invariants that depend on runtime state
where the authoritative state is available.

Express the main flow in named domain steps. Avoid hidden dependencies by making
ordering, state changes, and failure outcomes visible in the flow or the
operation's contract wherever they affect the next decision. Keep implementation
details inside the step that owns them.

Before adding a helper, type, or layer, check what it changes for a maintainer.
It should let a decision change independently, remove an assumption callers
must maintain, or make a necessary dependency explicit. Keep the simpler
structure when the abstraction only moves the same reasoning elsewhere.
