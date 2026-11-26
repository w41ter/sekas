# perf-lab

`perf-lab` runs isolated in-process three-voter clusters and measures a snapshot
isolation KV store by theme. The version 2 suite replaces the old ad-hoc cases.
Every case starts a fresh cluster. Matrix entries use distinct tables and record
parameters; entries within a case share the cluster and its storage history.
For strict independent comparisons, run separate invocations with one matrix
entry configured in each invocation.

## Run

List all built-in cases by theme, with each case's main test objective:

```sh
sekas perf-lab --list
sekas perf-lab --list --theme mvcc
```

`--list` exits without loading configuration or baselines, starting a cluster, or
writing reports. With no `--theme`, it lists every theme. It cannot be combined
with `--case` or `--all`.

Use release builds for performance measurements:

```sh
cargo run --release --bin sekas -- perf-lab --theme basic --conf conf/perf-lab-snapshot.toml
cargo run --release --bin sekas -- perf-lab --theme transaction --conf conf/perf-lab-snapshot.toml
cargo run --release --bin sekas -- perf-lab --case si-conflict --conf conf/perf-lab-snapshot.toml
cargo run --release --bin sekas -- perf-lab --all --conf conf/perf-lab-snapshot.toml
```

With no selection, only `basic` runs. `--case`, `--theme`, and `--all` are mutually
exclusive. `--all` includes long storage, contention, topology, and capacity
experiments. `conf/perf-lab-smoke.toml` has a small working set and short windows for
functional validation; its numbers are not capacity baselines. Case matrices are
fixed in code, so smoke runs use the same matrices and can still require substantial
fixture preparation, particularly for MVCC cases.
Basic successful point reads and scans require a working set larger than block
cache and write buffers, with SST bytes beyond the cache before measuring.
Seeded shared fixtures use distinct high-entropy values so compression cannot
collapse the physical working set. Read locality is determined by the case's
access distribution, rather than a memory/disk profile switch. The default and
snapshot configurations use a 32 MiB payload with a 1 MiB block cache; the smoke
configuration uses a smaller dataset and proportionally smaller cache and buffers.
`conf/perf-lab-disk.toml` remains usable without a separate working-set option.
These checks do not evict the OS page cache or guarantee physical disk I/O.

```sh
make perf-lab PERF_LAB_THEME=mvcc
make perf-lab PERF_LAB_THEME=basic PERF_LAB_BASELINE=/path/basic-baseline.json
```

## Cases and questions

| Theme | Cases | Controlled setup and question |
| --- | --- | --- |
| `basic` | `point-hit`, `point-miss` | Seeded/empty tables, validate presence; cost of successful and absent reads. |
| `basic` | `insert`, `update`, `delete` | Unique new keys, fixed live key set, and a finite deletion pass. Each seeded key is deleted once with disjoint worker ownership; the window ends when the key set is exhausted, and this destructive case skips warmup. |
| `basic` | `range-scan`, `cross-shard-scan` | Random scan starts and return-count matrix; full scan across two shards in distinct groups. Verify row counts. |
| `mvcc` | `latest-versions`, `snapshot-point` | Exact seeded version-count matrix, latest versus fixed historical timestamp; verify raw committed version count and returned payload. At most 64 live keys per entry. |
| `mvcc` | `snapshot-scan`, `tombstone-read` | Delete/update existing keys and insert keys after the snapshot. Validate historical scan count/payload and latest versus historical tombstone visibility. |
| `transaction` | `read-only-local`, `read-only-distributed` | One snapshot per operation, multiple distinct reads, controlled participant group count. |
| `transaction` | `blind-local`, `blind-distributed` | No snapshot read; vary key count and group count to compare local and distributed blind-write paths. |
| `transaction` | `read-write-local`, `read-write-distributed` | Read then write at SI; worker-owned key partitions prevent accidental conflicts. Record actual participants and payload bytes. |
| `transaction` | `large-txn` | Separate large-key-count matrix across configured participant counts; blind-write transaction cost. For interference with small requests use `small-large-txn`. |
| `contention` | `hotset` | Uniform blind updates on a hotset-size matrix; measures contention without claiming SI aborts. |
| `contention` | `si-conflict`, `si-conflict-retry` | Concurrent snapshot reads, configurable hold, write same hotset; measure aborts and bounded application retries. The single-key entry must observe conflicts. |
| `contention` | `long-short-txn` | Slow read/write transactions compete with short transactions; starvation/zero successful long transactions is a valid measured result. |
| `storage` | `gc-backlog`, `gc-steady` | Enable retention and periodic GC; seed an immutable user-key version probe, verify it actually loses versions while foreground updates run. Steady case extends to `storage_duration_secs`. |
| `storage` | `retention-boundary` | Old snapshot remains readable within retention, then its user version is reclaimed. This implementation has no active snapshot pinning; expired snapshots may return absence rather than an explicit expiration error. |
| `storage` | `insert-compaction`, `churn-compaction` | Sustained insertion/update/delete with 256 KiB write buffers and small L0 thresholds; require multiple completed flushes and a completed compaction in RocksDB EVENT_LOG. Record SST footprint and observed write stalls. |
| `mixed` | `read-update` | Independent arrival streams set offered read/write ratio, with separate latency/throughput reports. Achieved ratios can differ under saturation. |
| `mixed` | `read-scan`, `small-large-txn`, `import-online` | Foreground point reads/small transactions with background scans/large transactions/inserts in a separate table in the same group. Compare foreground alone versus with background. |
| `mixed` | `hot-cold` | Skewed writes to a small hotset and reads of a separate cold table in the same group. Measures interference, with independently reported workloads. |
| `topology` | `leader-transfer`, `data-leader-failover`, `root-failover` | Explicit transfer or stop leader, require changed route plus a successful real RPC. Root case also measures metadata requests and reports data-leader colocation. |
| `topology` | `follower-recovery` | Stop follower, write during absence, restart, transfer leadership to recovered replica, and read an acknowledged marker. |
| `topology` | `replica-add`, `replica-remove`, `snapshot-catchup` | Explicit membership changes, restore three voters after add tests. Snapshot case uses the existing force-new-peer-snapshot test knob and requires snapshot apply activity. |
| `topology` | `shard-migration`, `split-merge` | Seeded data and foreground traffic, verify ownership/convergence and data on both sides of the split. |
| `capacity` | `concurrency`, `offered-load` | Representative point reads, updates, distributed blind writes. Closed-loop concurrency matrix versus independent scheduled arrivals with bounded in-flight capacity. |
| `capacity` | `group-scale`, `node-scale` | Uniform traffic across actual distinct groups. Node case adds a server, explicitly moves leadership after replica catch-up, restores three voters, and measures before/after throughput. It measures deterministic assisted expansion, not automatic balancing policy. |
| `capacity` | `metadata-scale` | Exact per-database table-count matrix, metadata listing, point reads, and one create-table timing. Delete each stage's tables afterward. |

Each topology action runs with both point writes and distributed SI transactions.
In-process node stopping uses the server shutdown notifier, not SIGKILL, network
partition, or independent machines. Process-crash behavior belongs to the process
chaos harness. Normal client reads/writes allocate timestamps at root; the data
workloads should not be interpreted as a root-independent cached-routing path.

## Parameters

Common load and timing controls live under `[workload]`; cluster/database tuning
stays in the existing `[cluster.*]` sections. Case matrices are named constants at
the top of the owning case module, with no TOML configuration or `[cases]` section.
Background load parameters belong to the interference case using them rather than
borrowing another case's matrix.

| Parameter | Default | Purpose |
| --- | --- | --- |
| `concurrency`, `duration_secs`, `warmup_secs`, `cooldown_secs` | 32, 30, 5, 5 | Common measurement settings. Warmup reports are discarded; in-flight operations drain before measurement. |
| `key_space`, `value_size` | 4096, 8192 | Working set/payload. Basic successful reads and scans validate a fixture larger than cache and write buffers. |
| `conflict_hold_ms`, `retry_limit` | 5, 8 | Snapshot overlap and bounded retry behavior. |
| `request_timeout_secs`, `event_timeout_secs` | 10, 60 | Per-operation end-to-end deadline and event convergence budget. |
| `storage_duration_secs`, `gc_retention_ms`, `gc_interval_ms` | 120, 1000, 100 | Long storage windows and explicit GC configuration. |

The following source locations define the case parameters:

| Module | Case parameters |
| --- | --- |
| [`cases/basic.rs`](../src/bin/src/perf_lab/cases/basic.rs) | `SCAN_LIMITS = [16, 128, 1024]` for range scans. Cross-shard scans return the full fixture. |
| [`cases/mvcc.rs`](../src/bin/src/perf_lab/cases/mvcc.rs) | `VERSION_COUNTS = [1, 100, 1000]`. |
| [`cases/transaction.rs`](../src/bin/src/perf_lab/cases/transaction.rs) | `TXN_KEY_COUNTS = [1, 8, 32]`, `LARGE_TXN_KEY_COUNTS = [64, 256, 1024]`; named local, distributed, and large-transaction group matrices. Entries that cannot fit the distinct key space are skipped. |
| [`cases/contention.rs`](../src/bin/src/perf_lab/cases/contention.rs) | `HOTSET_SIZES = [1, 16, 1024]`. |
| [`cases/mixed.rs`](../src/bin/src/perf_lab/cases/mixed.rs) | Read/update ratios `[0.05, 0.5]` at 100 offered QPS, background scan limit 1024, and separate large-transaction candidates `[64, 256, 1024]`. |
| [`cases/capacity.rs`](../src/bin/src/perf_lab/cases/capacity.rs) | `CONCURRENCY_LEVELS = [1, 4, 16, 64]`, `OFFERED_QPS = [100, 1000, 5000]`, group-scale counts `[1, 2, 4]`, metadata counts `[1, 10, 100]`. Group/node scale each use their own concurrency constant of 64; node scale uses four groups. |

Removed matrix fields under `[workload]` are rejected rather than silently ignored.
Reports/baselines containing those fields must be regenerated. Actual operation
parameters remain recorded in workload reports.

Layout balancing and GC are disabled by the case registry unless required by the
scenario. Compaction cases use deliberately small buffers to make maintenance
observable; use their report configuration when interpreting results. They do
not prove production steady state solely by elapsed time. Increase duration,
data size, and repetitions when establishing production capacity baselines.

## Reports and regression checks

The output is `suite-<run_id>.json`, with `schema_version: 2`. Completed case
reports are preserved when another case fails. The suite continues through all
selected cases, records failed cases in `errors`, and exits nonzero if any fail.
Suites containing failed cases cannot become baselines. Old baseline names and
attempt-QPS semantics are incompatible and must be regenerated. The obsolete
checked-in baseline is removed rather than relabeled with new case names.

Workloads record successful QPS, attempt QPS, successes/failures, retry attempts,
conflicts, rows/bytes, longest gap between successful completions, all-outcome
latency and success-only latency, error categories, and phase summaries.
Latency histograms have bounded memory; bucket upper bounds are within 1/64 of
observed latency above 128 microseconds. AVG and MAX retain exact values.

Open-loop arrivals use scheduled timestamps for latency/deadlines, including
client scheduling delay. Arrivals exceeding the in-flight limit are counted as
`overload`, not silently omitted. Requests drain with deadlines when measurement
stops. A request is attributed to the phase in which it started.

Unexpected errors and empty workloads fail a case. SI conflict cases allow
expected transaction aborts. Offered-load cases allow overload/deadline errors
but retain these in the report. Fixture mismatches are always failures.

Create a baseline from an actual release run:

```sh
scripts/perf-lab-to-baseline.sh target/perf-lab/suite-123.json /path/basic-baseline.json
cargo run --release --bin sekas -- perf-lab --theme basic \
  --conf conf/perf-lab-snapshot.toml --baseline /path/basic-baseline.json --fail-on-regression
```

Compact baselines preserve cluster/workload configuration. Comparison rejects
incompatible build profiles, configurations and actual parameters, and checks successful throughput, AVG/P99 (including
phases), event durations, completion gaps, and unexpected failure rates.
`--fail-on-regression` also rejects a selected case missing from the baseline.
Error-rate limits use `report.max_failure_rate` (default zero). Baselines remain
host/build specific; collect multiple runs before treating short-window changes
as regressions. `scripts/perf-lab-diff.py` can inspect any derived metric.

## Functional validation (2026-10-09)

The complete debug smoke run exercised all 46 cases: 35 passed and 11 failed
strict scenario checks. This is functional coverage, not a release performance
baseline. The final three-voter node-scale implementation also passed a separate
run. The final large-transaction interference and disk point-read fixtures
also passed separate runs. Failures in the full run were:

- `large-txn`, `concurrency`: distributed transaction request deadlines.
- `si-conflict-retry`, `long-short-txn`: request deadlines beyond expected SI aborts.
- `gc-backlog`, `gc-steady`, `retention-boundary`: old user versions were not
  reclaimed within the event budget.
- `data-leader-failover`, `root-failover`: the router did not report a changed
  leader within the event budget.
- `follower-recovery`: a network error in the distributed workload.
- `snapshot-catchup`: no observed snapshot apply counter increase.

These failures still need diagnosis; they are not automatically attributed to
storage defects. The suite records them and exits nonzero. No failure allowance
was added to turn these observations into passing performance results.

Migration testing also exposed two concrete issues fixed alongside the lab:
the initial empty pull checkpoint must use the shard boundary, and raw scans at
the reserved intent version must not advance the local transaction read
watermark. Both have targeted regression tests.

可读性重构与三版本消融记录见 [实验报告](perf-lab-readability-ablation.md)。
输出字段的三版本消融、同份报告的体积对照和取舍理由见
[输出消融报告](perf-lab-output-ablation.md)。

## Case implementation

`CaseKind` identifies CLI selections and reports. `Command::build_cases()` converts
those selections into `Case` objects before running the suite. Each object owns
its identity and a typed `Scenario`; its theme is derived from that scenario.
`Case::configure()` fixes the common cluster settings, then applies settings owned
by the storage, topology or capacity scenario. `Case::run()` delegates to the
corresponding scenario method.

Theme implementations accept their own case types rather than the global
`CaseKind`. Transaction cases carry `mode`, `placement` and `key_matrix`, resolved
at construction. Data preparation still runs against the started cluster through
fixture/layout helpers, and topology actions retain their named methods. The suite
runner owns cluster startup, shutdown, error collection and baseline comparison.

To add a case, declare its stable identifier/name and registry entry, construct its
typed scenario in `Case::build()`, and implement the behavior in the owning theme.
The registry test checks CLI coverage and theme counts.
