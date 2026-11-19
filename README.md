# quorum

A Raft-based replicated key-value store in C++20, tested with deterministic simulation.

> **Status:** step 3 of the roadmap: leader election and log replication (Raft §5.2–5.4)
> on a deterministic simulated network. There is no key-value state machine yet.

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
./build/tests/quorum_tests replication_fuzz
```

The fuzz tests run 1,000 seeds each with random crashes, partitions, 10% message loss and
10% duplicate delivery while clients keep writing. Invariant checkers run after every tick:
Election Safety, Log Matching, State Machine Safety and Leader Completeness. A failure
prints its seed, and the same seed replays the same run exactly.

The code builds with `-Wall -Wextra -Werror`. CI runs on Ubuntu and macOS in Debug and
Release.

## Layout

```
include/quorum/sim/    deterministic RNG, tick clock, simulated network
include/quorum/raft/   Raft node, messages, storage, test cluster, invariant checkers
src/                   implementation
tests/                 test harness and tests
docs/                  design notes
```

## Roadmap

See [docs/VISION.md](docs/VISION.md).
