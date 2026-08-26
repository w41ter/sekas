# Sekas Chaos Design

## 1. Purpose

`sekas-chaos` is a deterministic, reproducible correctness-testing framework for Sekas. It runs
client workloads against an independently deployed multi-process cluster while a nemesis changes
process, network, disk, and server state.

The crate now includes the stable-state workload engine, Sekas executor, independent-process cluster
management, deterministic nemesis, admin actions, and the complete restore-before-verify runner.
Additional fault backends and CLI integration can be implemented incrementally without changing the
model described here.

The framework checks three properties:

1. Every operation published by a writer remains readable in its expected post-state.
2. Atomic batches and transactions never expose a state that is neither the complete pre-state nor
   the complete post-state.
3. After faults stop and every node recovers, a complete verification agrees with every writer's
   final stable state.

Linearizability and snapshot-isolation history checking remain the responsibility of
`sekas-checker`. They can consume optional histories emitted by this crate, but are not on the
online reader's critical path.

## 2. Architecture

```text
                         run seed + configuration
                                    |
                           +--------v--------+
                           |  Chaos Runner   |
                           +---+---------+---+
                               |         |
                 +-------------+         +-------------+
                 |                                         |
        +--------v---------+                       +-------v-------+
        | ClusterController|<----------------------|    Nemesis    |
        | one process/node |     typed actions     | seeded policy |
        +--------+---------+                       +---------------+
                 |
          Sekas endpoints
                 |
        +--------v------------------------------------------+
        |                     Workload                      |
        |  writers -> stable atomic progress <- readers    |
        +---------------------------------------------------+
```

The runner owns all components, cancellation, artifact collection, and cleanup. A failure in any
component cancels new work, but the runner still recovers active faults, restores the cluster, and
performs final verification when possible.

`ChaosRunner` accepts an async setup callback after deployment. The callback creates the database,
tables, `SekasExecutor`, and `WorkloadRunner` using the deployed node addresses. This keeps schema
choices in the workload while the runner retains ownership of cluster cleanup.

## 3. Cluster controller

`ClusterController` is the boundary between chaos logic and deployment mechanics. The initial
`LocalProcessCluster` backend will start every node as a separate `sekas start` child process with:

- an explicit node id, address, data directory, and join configuration;
- a retained process handle and PID;
- separate stdout/stderr logs;
- stable data directories across restart;
- a per-process fault-injection channel;
- best-effort cleanup on both success and failure.

Process state changes and database readiness are separate. For example, `start_node` completes when
the child process is running; `wait_ready` waits until the cluster is usable. This prevents control
operations from silently embedding assumptions about quorum or leader election.

Graceful stop, kill, and pause are distinct operations because they exercise different recovery
paths. Fault injection returns a `FaultHandle`; recovery consumes that exact handle. Active handles
are also tracked by the controller so shutdown can recover forgotten faults.

`LocalProcessCluster` implements this contract with `tokio::process::Child`. SIGINT, SIGKILL,
SIGSTOP, and SIGCONT have distinct methods; stdout and stderr are appended to stable per-node log
files. Its readiness probe is replaceable, with `SekasReadinessProbe` used in production and a fake
probe used by lifecycle tests. Future SSH, container, or remote-cluster backends implement the same
interface. The workload and nemesis must not inspect child processes directly.

The controller reports capabilities. Unsupported failpoint, network, and disk faults are rejected
explicitly and are never selected by the nemesis.

## 4. Determinism and artifacts

Every randomized decision derives from the run seed and a stable component identifier. A writer
operation is a pure function of `(seed, writer_id, sequence, stable_model)`. Because the stable model
is itself the deterministic result of the preceding plans, the complete stream is reproducible. The
nemesis uses a separate RNG stream derived from the run seed so changing the number of readers does
not change the fault schedule.

A run records at least:

- resolved configuration and seed;
- binary identity and node configurations;
- writer operation plans and execution/reconciliation results;
- every nemesis attempt with invocation and completion timestamps;
- process exit status and node logs;
- the first correctness violation and its observations;
- final verification results.

The in-memory event log is a bounded ring. Its capacity and dropped-event count are part of the
workload configuration and report. The seed, final writer counts, failures, and current expected
models remain available even when routine events have been evicted. A future artifact sink can
stream every event without changing the workload algorithm.

Artifacts must be sufficient to replay the same operation streams and fault actions. Wall-clock
timing may vary, so replay uses recorded ordering and relative scheduling points rather than claiming
bit-for-bit scheduling determinism.

## 5. Writer model

Each writer owns a disjoint logical namespace in the initial implementation. This makes its state a
single-writer deterministic state machine and makes failure reconciliation unambiguous. Shared-key
workloads may be added later with a stronger oracle.

For operation `n`, the writer knows both the complete pre-state and post-state of the operation's
verification target:

```text
generate plan(n)
       |
mark progress as writing
       |
execute
       |
       +-- success --------------------------+
       |                                     |
       +-- failure or unknown -> observe ----+
                                      |      |
                         +------------+------+
                         |            |      |
                       after        before   other
                         |            |      |
                      applied       retry  violation
                         |                   |
                         +---------+---------+
                                   |
                    publish stable progress n + 1
```

An RPC error is not evidence that an operation was not applied. On any failure or unknown outcome,
the writer reads the full verification target:

- exact post-state: treat the operation as applied;
- exact pre-state: retry the same deterministic operation;
- any other state: record a correctness violation before any optional repair.

A partial batch or transaction is therefore a violation, not a retry condition. Repair-after-report
may be useful for continuing a long run, but fail-fast is the default because repair must never hide
the first bad state.

Writer progress advances only after reconciliation reaches the complete post-state. A reader never
has to interpret request errors or uncertain operations.

## 6. Progress publication and reader protocol

Progress is process-local coordination and is intentionally not stored in Sekas. `WriterProgress`
encodes a seqlock in one `AtomicU64`:

- an even raw value is stable and contains the number of reconciled operations;
- an odd raw value means the writer is executing or reconciling its next operation.

The writer marks progress as writing *before* it can change database state and publishes the next
stable count only after reconciliation. Release/acquire ordering also publishes any in-memory model
updates made before the stable count.

A reader follows this protocol:

1. Acquire-load a randomly selected writer's progress.
2. If it is writing, select another writer or retry later.
3. Reconstruct the expected model at the published count.
4. Select a random verification target or range and read it from Sekas.
5. Acquire-load progress again.
6. Judge the observation only if both loads are the same stable value; otherwise discard it.

This avoids false violations when a writer changes data during a read without placing locks in the
workload path. Transaction and atomic-batch targets are observed using one Sekas snapshot. Point
operations may use ordinary point reads.

Readers verify current state at the published frontier, not the historical value immediately after
an old operation, because later operations may overwrite or delete the same key.

## 7. Operations

The initial operation set is:

- deterministic put;
- idempotent delete;
- atomic batch of absolute puts and deletes;
- transaction of absolute puts and deletes;
- atomic signed 64-bit add.

Put, delete, and absolute-value batch/transaction operations can be retried safely after observing
the pre-state. Atomic add needs special treatment: after an ambiguous timeout, observing the old
value does not prove that the original request cannot commit later. Retrying a raw add can apply the
delta twice.

The `AddI64` workload must not advance stable progress after an ambiguous result until one of these
mechanisms exists:

1. a server-side idempotency key;
2. a queryable transaction identity with a terminal result;
3. a compare-and-set form that includes the expected value or version.

Replacing add with an absolute put would be reconcilable but would not test the native atomic-add
path, so it is not considered equivalent. This is an explicit prerequisite, not an error to mask in
the writer retry loop.

## 8. Nemesis

`RandomNemesis` is deterministic for its seed and selects actions from the current `ClusterStatus`. It
must avoid invalid actions such as restarting an already running node or recovering an unknown
fault. Availability loss is allowed when configured, but the policy must distinguish deliberate
quorum loss from an accidental invalid schedule.

`max_unavailable_nodes` is a hard disruption budget. Once reached, only recovery and valid admin
actions can be selected. Stopped, exited, and paused nodes contribute start, restart, and resume
candidates respectively. Candidate ordering is stable and weighted sampling uses a dedicated RNG.

Implemented actions:

- graceful stop/start;
- kill/restart with the existing data directory;
- pause/resume;
- leader transfer and shard movement through administrative APIs.

Every event contains its sequence, invocation and completion times, before and after status, and an
outcome of succeeded, failed, or timed out. Events are kept in a bounded ring with a dropped count.

Network partitions, latency, packet loss, and disk errors require platform-specific fault backends
and are later milestones. All action attempts, including failures and no-op decisions, are recorded.

## 9. Shutdown and final verification

Normal completion and failure use the same staged shutdown:

1. stop generating new nemesis actions;
2. let in-flight nemesis actions finish or time out;
3. stop writers at stable boundaries;
4. recover all active faults and start required nodes;
5. wait for cluster readiness;
6. run a full verification of every writer's final stable model;
7. stop readers and the cluster;
8. write artifacts.

If a writer remains in an unknown state and cannot reconcile, the run is inconclusive or failed
according to configuration; its progress must not be advanced merely to permit shutdown.

## 10. Failure policy

The runner distinguishes:

- **correctness violation**: observed state is neither an allowed pre-state nor post-state;
- **infrastructure failure**: process controller or fault backend failed;
- **availability failure**: an operation could not complete within the configured recovery budget;
- **unsupported operation**: for example ambiguous `AddI64` without a resolution mechanism;
- **inconclusive**: final state could not be reconciled or verified.

These outcomes must not be collapsed into a single panic because they require different debugging
and CI policies.

## 11. Implementation milestones

1. **Contracts and progress primitive**: complete.
2. **Local process cluster**: complete, including child lifecycle, logs, stable directories,
   readiness, signals, restore, and cleanup.
3. **Basic stable workload**: complete, including put/delete writers, random readers, and final
   verification.
4. **Compound operations**: complete, including batch and transaction reconciliation with snapshot
   observations.
5. **Nemesis basics**: complete, including deterministic selection, graceful stop, kill, restart,
   pause, leader transfer, shard movement, disruption budgets, and event recording.
6. **Fault backends**: per-process failpoints, network faults, and disk faults.
7. **Atomic add**: only after ambiguous-result resolution is available.
8. **Orchestration and reports**: complete, including deploy, setup, workload/nemesis execution,
   restore-before-verify, combined reports, and shutdown.
9. **Replay and shrinking**: deterministic artifact replay, then minimize operation/fault schedules.

## 12. Non-goals of the first implementation

- proving arbitrary shared-key histories online;
- replacing `sekas-checker` linearizability or SI checking;
- exact replay of thread and network scheduling;
- silently repairing a correctness violation and reporting the run as valid;
- treating a graceful in-process shutdown as equivalent to a process crash.
