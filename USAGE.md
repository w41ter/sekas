# Sekas usage

Start a local node using the [quick start](README.md#quick-start).

## Shell

Connect the shell to a running node.

```sh
cargo run -- shell --addrs 127.0.0.1:21805
```

Type `HELP` to list commands or `HELP SET` to see the shell settings.

### Shell request tracing

Tracing defaults to off in each session. Use `SET TRACE ON` to print timing
trees for subsequent `GET`, `PUT`, and `DELETE` commands to stderr. Use
`SET TRACE OFF` to stop tracing subsequent commands.

```sql
CREATE DATABASE IF NOT EXISTS trace_demo;
CREATE TABLE IF NOT EXISTS trace_demo.kv;
SET TRACE ON;
PUT "a" "1" INTO trace_demo.kv;
GET "a" FROM trace_demo.kv;
SET TRACE OFF;
```

Run the shell and nodes from a version that supports trace replies. The nodes
return server spans in the RPC reply, and the shell displays them under the
corresponding client call. `Duration` is measured where the span ran. Remote
`Start` offsets are aligned to the caller's response receipt time, and client
RPC durations include the cost of collecting and returning the trace.

To also print sampled spans in node logs, start the nodes with
`sekas start --trace` alongside their usual arguments. Use `trace_id` to match
node log entries to the shell output.

## Benchmarks

With Sekas running, use the same preset for both phases. Load creates the
database and table if needed and populates the initial rows. Run executes the
configured workload against that table and reports throughput and latency.

From the repository root, load and run the uniform read-heavy preset.

```sh
workload=conf/workloads/read-heavy-uniform.toml
cargo run --release -- bench --conf "$workload" --phase load
cargo run --release -- bench --conf "$workload" --phase run
```

See the [benchmark workload guide](conf/workloads/README.md) for other presets,
configuration options, and Prometheus scraping. Use [perf-lab](docs/perf-lab.md)
for local performance baselines and regression checks.
