# Sekas perf-lab Baseline Report - 2026-09-28

## Scope

This report records a local short-window `perf-lab` baseline for the current
checkout. The run is intended as a lightweight branch baseline, not a capacity
benchmark.

- Commit: `f9d2e70`
- Working tree note: `scripts/rewrite_commit_messages.sh` was already untracked
  and was not used by this run.
- Host: `Mac16,7`, 14 logical CPUs, 48 GiB memory
- OS: macOS 26.4.1, build 25E253
- Rust: `rustc 1.99.0-nightly (0e29c21d9 2026-07-21)`
- Cargo: `cargo 1.99.0-nightly (3efb1f477 2026-07-17)`
- Config: `conf/perf-lab-snapshot.toml`
- Output directory: `target/perf-lab-baseline-20260928`
- Smoke validation: `target/perf-lab-baseline-smoke/single-key-update-1790581659625.json`

## Run Profile

The snapshot config starts an in-process 3-node cluster with 2 CPUs per node and
uses 4 runner threads. Workloads use concurrency 4, a 5 second measurement
window, 4 KiB default values, and a 4096-key key space. Read and scan cases seed
4096 keys before measurement.

The command pattern was:

```bash
target/debug/sekas perf-lab \
  --case <case> \
  --conf conf/perf-lab-snapshot.toml \
  --out-dir target/perf-lab-baseline-20260928
```

All selected cases completed with zero workload failures.

## Main Results

Latency values are microseconds.

| Case | Workload | Ops | QPS | Avg | P50 | P95 | P99 | Max | Failures |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `single-key-update` | `single_key_update` | 11,388 | 2,276.85 | 1,616 | 1,482 | 2,301 | 4,724 | 15,556 | 0 |
| `batch-txn-commit` | `batch_txn_commit` | 11,759 | 2,350.96 | 1,562 | 1,508 | 2,005 | 2,461 | 18,307 | 0 |
| `point-read` | `point_read` | 17,152 | 3,429.51 | 1,030 | 1,018 | 1,330 | 1,507 | 3,490 | 0 |
| `mixed-read-write` | `mixed_read_write` | 16,187 | 3,236.60 | 1,099 | 1,061 | 1,508 | 1,766 | 9,554 | 0 |
| `prefix-scan` | `prefix_scan` | 4,089 | 817.60 | 4,748 | 4,631 | 6,564 | 8,130 | 20,294 | 0 |
| `txn-conflict` | `txn_conflict` | 11,239 | 2,247.31 | 1,640 | 1,558 | 2,162 | 2,873 | 18,020 | 0 |
| `multi-key-txn` | `multi_key_txn` | 8,604 | 1,720.43 | 2,179 | 1,850 | 3,875 | 8,125 | 28,958 | 0 |
| `hotspot-update-diagnostics` | `hotspot_single_key` | 12,697 | 2,538.94 | 1,435 | 1,406 | 1,828 | 2,032 | 4,982 | 0 |
| `hotspot-update-diagnostics` | `hotspot_small_hotset` | 12,761 | 2,551.51 | 1,428 | 1,399 | 1,813 | 2,039 | 4,831 | 0 |
| `hotspot-update-diagnostics` | `hotspot_wide_hotset` | 12,430 | 2,485.73 | 1,470 | 1,406 | 1,823 | 2,090 | 57,388 | 0 |

## Matrix Results

### Value Size Matrix

| Value size | Ops | QPS | P99 us | Max us | Payload throughput |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16 B | 14,556 | 2,910.80 | 1,999 | 9,039 | 45.5 KiB/s |
| 128 B | 14,288 | 2,856.91 | 2,303 | 10,031 | 357.1 KiB/s |
| 1 KiB | 14,325 | 2,864.67 | 1,945 | 5,752 | 2.80 MiB/s |
| 16 KiB | 8,252 | 1,650.07 | 3,031 | 19,969 | 25.78 MiB/s |

QPS stays near 2.86k through 1 KiB values, then drops to 1.65k at 16 KiB.
Payload throughput still increases with larger values, reaching about 25.78
MiB/s at 16 KiB.

### Multi-key Transaction Matrix

| Keys per txn | Ops | QPS | Avg us | P95 us | P99 us | Max us |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 14,173 | 2,834.31 | 1,271 | 1,667 | 1,922 | 4,729 |
| 2 | 13,186 | 2,636.71 | 1,378 | 1,820 | 2,160 | 17,861 |
| 4 | 10,719 | 2,142.63 | 1,639 | 2,179 | 2,783 | 22,447 |
| 8 | 8,431 | 1,685.77 | 2,235 | 3,015 | 4,208 | 118,804 |
| 16 | 5,254 | 1,050.61 | 3,670 | 5,129 | 6,386 | 17,967 |

Throughput decreases monotonically as each transaction touches more keys: from
2.83k QPS at 1 key to 1.05k QPS at 16 keys. P99 latency rises from 1.92 ms to
6.39 ms.

### Read/Scan Prepare Cost

| Case | Seed keys | Seed bytes | Prepare duration |
| --- | ---: | ---: | ---: |
| `point-read` | 4,096 | 16 MiB | 3.58 s |
| `mixed-read-write` | 4,096 | 16 MiB | 3.64 s |
| `prefix-scan` | 4,096 | 16 MiB | 2.69 s |

## Observations

- The cleanest read baseline is `point-read`: 3.43k QPS with 1.51 ms P99.
- `mixed-read-write` keeps most of the point-read throughput at 3.24k QPS and
  1.77 ms P99 under a 10 percent write ratio.
- `prefix-scan` is the highest-latency steady workload in this set: 818 QPS and
  8.13 ms P99 for limit-128 scans.
- Transaction cost scales visibly with key count. The single `multi-key-txn`
  case with 8 keys measured 1.72k QPS and 8.13 ms P99; the matrix's 8-key leg
  measured 1.69k QPS and 4.21 ms P99, with one 118.8 ms max-latency outlier.
- Hotspot random puts were stable across single-key, 16-key, and 1024-key
  hotsets, all near 2.5k QPS and about 2.0 ms P99. The 1024-key hotset had a
  57.4 ms max-latency outlier.

## Raw Reports

- `batch-txn-commit-1790581707055.json`
- `hotspot-update-diagnostics-1790581813800.json`
- `mixed-read-write-1790581725492.json`
- `multi-key-txn-1790581754826.json`
- `multi-key-txn-matrix-1790581785386.json`
- `point-read-1790581714079.json`
- `prefix-scan-1790581737074.json`
- `single-key-update-1790581698905.json`
- `txn-conflict-1790581747531.json`
- `value-size-matrix-1790581762512.json`

These files were condensed into `conf/perf-lab-baseline.json` as the checked-in
suite baseline. For new runs, keep the generated `suite-<run_id>.json` as the
next `--baseline` input when it is better than the current baseline. The
configured comparison thresholds are 5 percent maximum QPS drop and 10 percent
maximum P99 latency increase.

