# Sekas machine benchmark

`sekas-machine-bench` measures local latency primitives that commonly contribute to Sekas request
latency. It does not start a Sekas cluster and does not perform cross-machine tests.

Build the release binary before measuring so compilation does not overlap the benchmark:

```bash
cargo build --release -p sekas-machine-bench
target/release/sekas-machine-bench \
  --disk-dir /path/to/sekas-data \
  --json /tmp/sekas-machine-baseline.json
```

The default run uses one second of warm-up and three seconds of measurement per case. For a quick
functional check:

```bash
target/release/sekas-machine-bench \
  --duration-secs 1 \
  --warmup-secs 0 \
  --file-size-mib 16
```

The storage benchmark creates and removes a uniquely named temporary file under `--disk-dir`.
Always point this option at the filesystem used by the intended Sekas data directory. The direct I/O
rows have distinct semantics:

- `odirect_read`: random aligned 4 KiB reads that bypass the page cache.
- `odirect_write`: aligned 4 KiB writes that bypass the page cache but do not request durability.
- `odirect_dsync_write`: aligned 4 KiB writes using both `O_DIRECT` and `O_DSYNC`.
- `fdatasync_write`: buffered positional writes followed by `fdatasync`.

Unsupported operations are reported as `SKIPPED`; one unsupported case does not discard the other
machine results. The optional JSON report contains the environment fingerprint and complete bounded
histograms used to generate the human-readable table.
