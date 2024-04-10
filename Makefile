# set makefile echo back
ifdef VERBOSE
	V :=
else
	V := @
endif

GRCOV := $(shell command -v grcov 2> /dev/null)

.PHONY: build
## build : Build binary
build:
	$(V)cargo build

.PHONY: etcd
## etcd : Run etcd server
etcd:
	cargo run --all-features -- start --db /tmp/store --addr '127.0.0.1:2379' --init

.PHONY: lint
## lint : Lint codespace
lint:
	$(V)cargo clippy --workspace --tests --all-features -- -D warnings

.PHONY: fmt
## fmt : Format all code
fmt:
	$(V)cargo fmt --all -- --check

.PHONY: test
## test : Run test
test:
	$(V)cargo test --workspace -- $(FILTER)

.PHONY: clean
## clean : Clean build env
clean:
	$(V)cargo clean

.PHONY: coverage
## coverage : Run test with coverage
coverage:
ifndef GRCOV
	$(error "grcov is not avaiable, please install it by: cargo install grcov")
endif

	# $(V)CARGO_INCREMENTAL=0
	$(V)RUSTFLAGS="-Zprofile" \
		cargo test --workspace -- $(FILTER)
	$(V)grcov . -s . --binary-path ./target/debug/ \
		-t lcov \
		--branch \
		--ignore-not-existing \
		--ignore "/*" \
		-o ./target/debug/coverage/lcov.info
	$(V)genhtml -o ./target/debug/coverage/ \
		--show-details \
		--highlight \
		--ignore-errors source \
		--legend ./target/debug/coverage/lcov.info
	$(V)echo "the coverage report is generated in: ./target/debug/coverage"

.PHONY: help
## help : Print help message
help: Makefile
	@sed -n 's/^##//p' $< | awk 'BEGIN {FS = ":"} {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
