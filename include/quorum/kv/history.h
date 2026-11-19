#pragma once

// Given: step 5's linearizability checker needs every client operation's
//        real-time interval and outcome.
// Approach: an append-only record per operation: its command, invoke tick,
//           and (once a reply arrives) completion tick and result. An op
//           with no completion was still pending, or its client gave up. A
//           checker must treat it as possibly applied.

#include <cstddef>
#include <optional>
#include <vector>

#include "quorum/kv/command.h"
#include "quorum/sim/clock.h"

namespace quorum::kv {

using sim::Tick;

struct OpRecord {
    Command command;  // includes client and seq
    Tick invoked = 0;
    std::optional<Tick> completed;
    std::optional<Result> result;
};

class History {
public:
    std::size_t invoke(Command command, Tick now) {
        ops_.push_back(OpRecord{std::move(command), now, std::nullopt, std::nullopt});
        return ops_.size() - 1;
    }
    // The first completion wins. A later duplicate reply is ignored.
    void complete(std::size_t op, Tick now, Result result) {
        OpRecord& r = ops_.at(op);
        if (r.completed) return;
        r.completed = now;
        r.result = std::move(result);
    }
    const std::vector<OpRecord>& ops() const { return ops_; }

private:
    std::vector<OpRecord> ops_;
};

}  // namespace quorum::kv
