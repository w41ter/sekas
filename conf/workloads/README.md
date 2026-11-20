# Benchmark workloads

Each preset uses a dedicated table. Its TOML file sets the run budget and
key counts. Run load even for a preset that starts empty so Sekas can
create the table.

## Choose a preset

| Preset | Operation mix | Use it to measure |
| --- | --- | --- |
| [Read only](read-only.toml) | 100% reads, uniform keys | Point-read throughput and latency |
| [Read heavy, uniform](read-heavy-uniform.toml) | 95% reads, 5% updates, uniform keys | Point reads with occasional writes |
| [Read heavy, Zipfian](read-heavy-zipfian.toml) | 95% reads, 5% updates, Zipfian keys | The effect of hot keys compared with the uniform preset |
| [Balanced read/update](read-update-balanced.toml) | 50% reads, 50% updates, uniform keys | Point access under sustained writes |
| [Transactional insert](txn-insert.toml) | 100% multi-key inserts | Throughput while growing a table |
| [Transactional update](txn-update.toml) | 100% multi-key updates | Multi-key writes on existing data |
| [Scan heavy](scan-heavy.toml) | 90% scans, 10% reads | Short snapshot scans |
| [CAS](cas.toml) | 100% version-based CAS, Zipfian keys | Update throughput and version rejections under contention |
| [Insert if absent](insert-if-absent.toml) | 100% conditional inserts, uniform keys | Create-once writes and duplicate rejections |
| [Atomic increment](atomic-increment.toml) | 100% increments, Zipfian keys | Contended counter throughput |

## Load and run

Use the same TOML file for both phases. Load creates the database and table
if needed, then upserts the initial rows. Run uses that table and picks
operations by their relative weights. [Quick start](../../README.md#benchmarks)

Inserts and updates use unconditional puts. Inserts start at
`workload.insert_start`, which defaults to `workload.records`. When repeating
an insert benchmark on the same table, choose an unused range to keep growing
the table. Reusing the range overwrites those rows.

Reports show successful operations per second, average and p99 latency for
successful operations, condition rejections, and errors. A CAS read and write
together count as one operation, as does a write transaction or scan. A
condition rejection counts toward the operation budget. The command prints
the totals and exits with an error if there are any reported errors.

### Scrape client metrics

Pass `--metrics-addr` to serve the benchmark process's Prometheus metrics
while it runs. By default, the benchmark opens no metrics listener.

```sh
sekas bench --conf conf/workloads/read-heavy-uniform.toml --metrics-addr 127.0.0.1:9180
```

Set the Prometheus target to the benchmark host and port and set
`metrics_path: /admin/metrics`. For a remote scraper, bind to an address
it can reach. In Grafana, select that scrape job under **Client jobs**.

## Tune a preset

- `operations` sets the number of attempts in each run across all workers.
- `request_timeout_ms` sets the client timeout for a transaction or read.
  It defaults to 500 milliseconds.
- `workload.records` sets the initial row count. Run load again after increasing it.
- `[workload.mix]` sets relative weights for `read`, `insert`, `update`,
  `scan`, `cas`, `insert_if_absent`, and `increment`.
  Omitted operations have zero weight.
- `workload.distribution` controls how the benchmark chooses read keys, update
  start keys, and scan start keys from the initial rows. Uniform spreads access
  evenly. Zipfian favors hot keys.
- `workload.txn_keys` sets the rows per insert or update transaction and the
  maximum rows per load batch. Updates use consecutive keys and wrap within
  the initial range.
- `workload.scan_length` caps the rows returned by each snapshot scan.

## CAS, create-once writes, and counters

These operations each touch one key. CAS reads an initial row's version and
commits the new value if that version still matches. It makes one attempt per
operation.

Insert-if-absent and atomic increment each sample from their own pool of
`workload.shared_keys` keys using the configured distribution. The two pools
are also separate from the initial rows and ordinary inserts. Insert-if-absent
creates a value only when the key is absent. Repeated runs reuse the pool,
so keys created by earlier runs will be rejected. Atomic increment adds 1 to
a signed 64-bit counter, starting at zero when the key is absent. Repeated runs
continue the counters.

For a configuration with reads, inserts, updates, and scans, see the [mixed example](../default-bench.toml).
