#include "quorum/raft/messages.h"

#include <cstdint>
#include <string>

#include "quorum/codec/text.h"

namespace quorum::raft {

namespace {

template <class... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};

std::optional<RaftMessage> decode_ae(codec::Reader& r) {
    const auto term = r.u64();
    const auto leader = r.u32();
    const auto pidx = r.u64();
    const auto pterm = r.u64();
    const auto commit = r.u64();
    const auto n = r.u64();
    if (!term || !leader || !pidx || !pterm || !commit || !n) return std::nullopt;
    // Every entry takes at least 5 bytes (" 0 0 " for an empty command), so a
    // count that can't fit is rejected before anything is allocated.
    if (*n > r.remaining() / 5) return std::nullopt;
    AppendEntries m{*term, *leader, *pidx, *pterm, *commit, {}};
    m.entries.reserve(*n);
    for (std::uint64_t i = 0; i < *n; ++i) {
        const auto et = r.u64();
        if (!et) return std::nullopt;
        const auto cmd = r.bytes();
        if (!cmd) return std::nullopt;
        m.entries.push_back(LogEntry{*et, std::string(*cmd)});
    }
    return m;
}

}  // namespace

Term term_of(const RaftMessage& msg) {
    return std::visit([](const auto& m) { return m.term; }, msg);
}

std::string encode(const RaftMessage& msg) {
    return std::visit(
        Overloaded{
            [](const RequestVote& m) {
                return codec::Writer("RV")
                    .u64(m.term)
                    .u64(m.candidate_id)
                    .u64(m.last_log_index)
                    .u64(m.last_log_term)
                    .str();
            },
            [](const RequestVoteReply& m) {
                return codec::Writer("RVR").u64(m.term).flag(m.vote_granted).str();
            },
            [](const AppendEntries& m) {
                codec::Writer w("AE");
                w.u64(m.term)
                    .u64(m.leader_id)
                    .u64(m.prev_log_index)
                    .u64(m.prev_log_term)
                    .u64(m.leader_commit)
                    .u64(m.entries.size());
                for (const LogEntry& e : m.entries) w.u64(e.term).bytes(e.command);
                return w.str();
            },
            [](const AppendEntriesReply& m) {
                return codec::Writer("AER")
                    .u64(m.term)
                    .flag(m.success)
                    .u64(m.match_index)
                    .u64(m.last_log_index)
                    .str();
            },
        },
        msg);
}

std::optional<RaftMessage> decode(std::string_view payload) {
    codec::Reader r(payload);
    const auto tag = r.token();
    if (!tag) return std::nullopt;

    std::optional<RaftMessage> out;
    if (*tag == "RV") {
        const auto term = r.u64();
        const auto cand = r.u32();
        const auto idx = r.u64();
        const auto lterm = r.u64();
        if (term && cand && idx && lterm) out = RequestVote{*term, *cand, *idx, *lterm};
    } else if (*tag == "RVR") {
        const auto term = r.u64();
        const auto granted = r.flag();
        if (term && granted) out = RequestVoteReply{*term, *granted};
    } else if (*tag == "AE") {
        out = decode_ae(r);
    } else if (*tag == "AER") {
        const auto term = r.u64();
        const auto ok = r.flag();
        const auto match = r.u64();
        const auto last = r.u64();
        if (term && ok && match && last) out = AppendEntriesReply{*term, *ok, *match, *last};
    }
    if (!out || !r.done()) return std::nullopt;
    return out;
}

}  // namespace quorum::raft
