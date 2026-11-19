#include "quorum/kv/state_machine.h"

#include <stdexcept>

namespace quorum::kv {

std::optional<Applied> KvStateMachine::apply(LogIndex index, const raft::LogEntry& entry) {
    if (index != last_applied_ + 1) throw std::logic_error("KvStateMachine: applied out of order");
    last_applied_ = index;

    if (entry.is_noop()) {
        ++stats_.noops;
        return std::nullopt;
    }
    const auto cmd = decode_command(entry.command);
    if (!cmd) {
        ++stats_.malformed;
        return std::nullopt;
    }
    Session& s = sessions_[cmd->client];
    if (cmd->seq < s.seq) {
        ++stats_.stale;
        return std::nullopt;
    }
    if (cmd->seq == s.seq) {
        ++stats_.duplicates;
        return Applied{cmd->client, cmd->seq, s.result, true};
    }
    const Result r = execute(*cmd);
    s = Session{cmd->seq, r};
    executions_.push_back(Execution{index, *cmd, r});
    ++stats_.executed;
    return Applied{cmd->client, cmd->seq, r, false};
}

Result KvStateMachine::execute(const Command& c) {
    const auto it = data_.find(c.key);
    const std::optional<std::string> current =
        it == data_.end() ? std::nullopt : std::optional<std::string>{it->second};
    switch (c.op) {
        case Op::Put:
            data_[c.key] = c.value;
            return Result{true, std::nullopt};
        case Op::Get:
            return Result{true, current};
        case Op::Delete:
            if (it != data_.end()) data_.erase(it);
            return Result{current.has_value(), std::nullopt};
        case Op::Cas:
            if (current == c.expected) {
                data_[c.key] = c.value;
                return Result{true, current};
            }
            return Result{false, current};
    }
    return Result{};
}

std::optional<Result> KvStateMachine::cached(ClientId client, Seq seq) const {
    const auto it = sessions_.find(client);
    if (it == sessions_.end() || it->second.seq != seq) return std::nullopt;
    return it->second.result;
}

bool KvStateMachine::is_stale(ClientId client, Seq seq) const {
    const auto it = sessions_.find(client);
    return it != sessions_.end() && seq < it->second.seq;
}

}  // namespace quorum::kv
