# quorum

A Raft-based replicated key-value store in C++20, tested with deterministic simulation.

> **Status:** step 5 of the roadmap: a replicated key-value store (put, get, delete, cas)
> with exactly-once client requests, on Raft (§5.2–5.4, §8), on a deterministic simulated
> network. Every fuzzed client history is checked for linearizability.

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
./build/tests/quorum_tests kv_
```

The fuzz tests run 1,000 seeds each with random crashes, partitions, 10% message loss and
10% duplicate delivery while clients keep writing. Invariant checkers run after every tick:
Election Safety, Log Matching, State Machine Safety and Leader Completeness. The KV fuzz
also checks every client history for linearizability, with a Wing & Gong/Porcupine-style
checker that explains any failure. It checks that every replica ends up with the same map,
and that every write executed exactly once. A failure prints its seed, and the same seed replays the same run
exactly.

Each fuzz test runs `QUORUM_FUZZ_SEEDS` seeds (default 1000), for example
`QUORUM_FUZZ_SEEDS=200 ./build/tests/quorum_tests`. CI uses 200 in Debug and 1000 in Release.

The code builds with `-Wall -Wextra -Werror`. CI runs on Ubuntu and macOS in Debug and
Release.

## Layout

```
include/quorum/sim/    deterministic RNG, tick clock, simulated network
include/quorum/codec/  strict text codec shared by every wire format
include/quorum/raft/   Raft node, messages, storage, services, test cluster, invariant checkers
include/quorum/kv/     KV state machine, server, simulated client, history, KV cluster
src/                   implementation
tests/                 test harness and tests
docs/                  design notes
```

## Roadmap

See [docs/VISION.md](docs/VISION.md).
