# Sekas

Sekas is a distributed transactional key-value store with ordered keys and
versioned values.

- **Conditional transactions across shards.** Atomically put or delete multiple
  keys across shards. Writes can check key existence, value, or version. The
  transaction commits only if all its conditions hold. [Transaction design](docs/txn.md)
- **Point reads and scans at a chosen version.** Use the same retained snapshot
  version to look up individual keys and scan key ranges or prefixes across shards,
  even while concurrent writes change the latest values. [Range API](src/client/src/range.rs)
- **Versioned key watches.** Subscribe to changes on an individual key from a
  specified version. The stream replays retained changes from that version and
  delivers new updates as they occur. [Rust client API](src/client/src/database.rs)

## Architecture

![topology][topology]

See [design doc][design-doc] for more details.

[topology]: ./docs/img/topology.drawio.svg
[design-doc]: ./docs/design.md

## Quick start

1. Build

```sh
make build
```

2. Deploy a single node cluster

```sh
cargo run -- start --init
```

3. Verify

```sh
cargo run -- shell
```

For shell tracing and benchmarks, see the [usage guide](USAGE.md).

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Sekas project.

## Community

For informal discussions, please go to the [forum](https://github.com/w41ter/sekas/discussions).

