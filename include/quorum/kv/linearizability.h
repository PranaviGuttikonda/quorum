#pragma once

// Given: a History of client operations (invoke tick, completion tick or
//        none, command, result) recorded against the replicated KV store.
// Approach: decide whether some total order of the operations
//           (a) respects real time: if A completed before B was invoked, A
//               comes first, and
//           (b) replays through a sequential KV map giving every completed
//               operation the result it actually returned.
//   - Per key: every operation touches exactly one key, and linearizability
//     is compositional, so each key is checked on its own (P-compositionality).
//   - Search: Wing & Gong's backtracking over call/return events, with Lowe's
//     memoization of (set of linearized ops, model state), as in Porcupine and
//     Knossos.
//   - Incomplete operations: one with no reply may have taken effect at any
//     point after its invoke, or never. Its return is placed after
//     everything, its result is unconstrained, and the search succeeds once
//     every completed operation is placed. An incomplete get has no effect and
//     is dropped.
//   - Ties: ticks are ordered so that, within a tick, replies are delivered
//     before new operations are invoked (that's how KvCluster runs). So
//     completed == invoked counts as "before".
//   - Bound: each key's search stops after max_states memoized
//     configurations, and the verdict is then Unknown, never a pass.
//   - On failure the report is minimal and readable:
//       1. the culprit, which is the operation completing at the end of the
//          shortest real-time prefix that is already non-linearizable
//          (linearizability is prefix-closed, so that is sound);
//       2. only the operations needed to keep that prefix failing. It drops
//          those that can't change the key, and writes overwritten by a later
//          put before anything could observe them. Removing either kind can
//          never create a failure, so what remains is a true witness;
//       3. the values the key could hold whenever the culprit could take
//          effect.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "quorum/kv/history.h"

namespace quorum::kv {

enum class Verdict { Linearizable, NotLinearizable, Unknown };

const char* to_string(Verdict v);

struct CheckOptions {
    std::uint64_t max_states_per_key = 1'000'000;
};

struct KeyReport {
    std::string key;
    Verdict verdict = Verdict::Linearizable;
    std::uint64_t ops = 0;
    std::uint64_t states = 0;  // memoized configurations explored
    std::string explanation;   // set unless Linearizable
};

struct CheckResult {
    Verdict verdict = Verdict::Linearizable;  // NotLinearizable > Unknown > Linearizable
    std::vector<KeyReport> keys;              // one per key, in key order
    std::uint64_t ops = 0;
    std::uint64_t states = 0;
    // The first non-linearizable key's explanation, else the first unknown's.
    std::string explanation;
};

// Throws std::invalid_argument for a completion before its invoke, or a
// completed operation without a result.
CheckResult check_linearizable(const std::vector<OpRecord>& history, CheckOptions options = {});

// The sequential model: what `c` does to a key currently holding `state`.
struct ModelStep {
    std::optional<std::string> next;
    Result result;
};
ModelStep model_step(const std::optional<std::string>& state, const Command& c);

}  // namespace quorum::kv
