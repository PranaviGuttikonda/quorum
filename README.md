# quorum

A Raft-based replicated key-value store in C++20, tested with deterministic simulation.

> **Status:** step 1 of the roadmap: build setup and a deterministic simulated network.
> There is no Raft logic yet.

## Why deterministic simulation

Bugs in distributed systems usually show up only under rare orderings of delays, message
losses, partitions and crashes. In quorum, every source of nondeterminism (time,
randomness, the network) goes through a single seeded simulator. A failing run can then be
replayed exactly from its seed, and thousands of adversarial schedules can run in seconds.

## Building and testing

Requirements: CMake ≥ 3.20 and a C++20 compiler (GCC or Clang).

```sh
cmake -S . -B build
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

To run only some tests, pass a substring of their names:

```sh
./build/tests/quorum_tests network_partition
```

The code builds with `-Wall -Wextra -Werror`. CI runs on Ubuntu and macOS in Debug and
Release.

## Layout

```
include/quorum/   public headers
src/              implementation
tests/            test harness and tests
docs/             design notes
```

## Roadmap

See [docs/VISION.md](docs/VISION.md).
