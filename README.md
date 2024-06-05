# Sekas

Sekas is a fault-tolerant, scalable, distributed transactional key-value store, used as a cache, database, and storage engine for other distributed systems.

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

Run and enjoy it.

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Sekas project.

## More information

For informal discussions, please go to the [forum](https://github.com/w41ter/sekas/discussions).

