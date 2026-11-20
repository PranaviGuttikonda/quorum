# Vision

quorum is a replicated key-value store built on Raft. Its main goal is correctness that can
be shown, not just claimed. Every component runs inside a deterministic simulator, so
whole-cluster behaviour can be tested against arbitrary faults, and any failing run can be
replayed exactly from its seed.

## Principles

- **One source of nondeterminism.** All randomness comes from a seeded, portable RNG.
  All time is simulated ticks. All communication goes through the simulated network.
  Protocol code never reads the wall clock, `std::random_device` or the OS.
- **Portable reproducibility.** A seed produces the same run on every platform and
  standard library. That is why the project doesn't use `std::` distributions.
- **Protocol code is I/O-free.** Raft nodes are state machines driven by messages and
  ticks. A real transport can replace the simulated one later without changing protocol
  logic.
- **Small, reviewed steps.** Each phase below lands as its own issue and pull request,
  with tests.

## Roadmap

0. **Foundation** *(done)*: CMake build, test harness, CI, a seeded RNG, a tick clock,
   and a simulated network with delay, loss, partitions and crash/restart.
1. **Leader election** *(done)*: terms, `RequestVote`, randomized election timeouts and
   heartbeats. Safety check: at most one leader per term.
2. **Log replication** *(done)*: `AppendEntries`, the log-matching property, commit index
   advancement, and follower catch-up after lag.
3. **Key-value state machine** *(done)*: `get` / `put` / `delete` applied from committed entries,
   a client API with leader redirection, and request de-duplication.
4. **Fault testing** *(done)*: randomized simulations that run many seeds with partitions,
   crashes, message loss and reordering. Invariant checkers cover election safety, log
   matching, leader completeness and state-machine safety. They print a minimal
   reproducing seed on failure.
5. **Persistence** *(durable storage and crash recovery done; snapshots and log
   compaction not started)*: durable term, vote and log through a storage interface, a simulated
   disk with crash-consistency faults, and snapshots with log compaction.
6. **Visualizer** *(current: a live browser view of the real engine in WebAssembly)*: export simulation traces and step through a run to see elections,
   replication and faults over time.
7. **Benchmarks**: throughput and latency under varying cluster sizes, delays and loss
   rates, plus a real-network transport for comparison.
