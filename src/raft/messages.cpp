#include "quorum/raft/messages.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <sstream>
#include <vector>

namespace quorum::raft {

namespace {

template <class... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};

// Given: a payload of single-space-separated fields.
// Approach: split on ' ', rejecting empty fields, so leading, trailing or
//           doubled spaces all fail.
std::optional<std::vector<std::string_view>> split(std::string_view s) {
    std::vector<std::string_view> out;
    std::size_t start = 0;
    for (;;) {
        const std::size_t sp = s.find(' ', start);
        const std::string_view field = s.substr(start, sp - start);
        if (field.empty()) return std::nullopt;
        out.push_back(field);
        if (sp == std::string_view::npos) return out;
        start = sp + 1;
    }
}

// Approach: from_chars accepts no sign, whitespace or prefix, so "+1", " 1"
//           and "0x1" are all rejected. The whole field must be consumed.
std::optional<std::uint64_t> parse_u64(std::string_view f) {
    std::uint64_t v = 0;
    const auto [ptr, ec] = std::from_chars(f.data(), f.data() + f.size(), v);
    if (ec != std::errc{} || ptr != f.data() + f.size()) return std::nullopt;
    return v;
}

std::optional<NodeId> parse_node(std::string_view f) {
    const auto v = parse_u64(f);
    if (!v || *v > std::numeric_limits<NodeId>::max()) return std::nullopt;
    return static_cast<NodeId>(*v);
}

std::optional<bool> parse_bool(std::string_view f) {
    if (f == "0") return false;
    if (f == "1") return true;
    return std::nullopt;
}

}  // namespace

Term term_of(const RaftMessage& msg) {
    return std::visit([](const auto& m) { return m.term; }, msg);
}

std::string encode(const RaftMessage& msg) {
    std::ostringstream os;
    std::visit(Overloaded{
                   [&](const RequestVote& m) {
                       os << "RV " << m.term << ' ' << m.candidate_id << ' ' << m.last_log_index
                          << ' ' << m.last_log_term;
                   },
                   [&](const RequestVoteReply& m) {
                       os << "RVR " << m.term << ' ' << (m.vote_granted ? 1 : 0);
                   },
                   [&](const AppendEntries& m) {
                       os << "AE " << m.term << ' ' << m.leader_id << ' ' << m.prev_log_index << ' '
                          << m.prev_log_term << ' ' << m.leader_commit;
                   },
                   [&](const AppendEntriesReply& m) {
                       os << "AER " << m.term << ' ' << (m.success ? 1 : 0);
                   },
               },
               msg);
    return os.str();
}

std::optional<RaftMessage> decode(std::string_view payload) {
    const auto fields = split(payload);
    if (!fields || fields->empty()) return std::nullopt;
    const auto& f = *fields;
    const std::string_view tag = f[0];

    if (tag == "RV" && f.size() == 5) {
        const auto term = parse_u64(f[1]);
        const auto cand = parse_node(f[2]);
        const auto idx = parse_u64(f[3]);
        const auto lterm = parse_u64(f[4]);
        if (!term || !cand || !idx || !lterm) return std::nullopt;
        return RequestVote{*term, *cand, *idx, *lterm};
    }
    if (tag == "RVR" && f.size() == 3) {
        const auto term = parse_u64(f[1]);
        const auto granted = parse_bool(f[2]);
        if (!term || !granted) return std::nullopt;
        return RequestVoteReply{*term, *granted};
    }
    if (tag == "AE" && f.size() == 6) {
        const auto term = parse_u64(f[1]);
        const auto leader = parse_node(f[2]);
        const auto pidx = parse_u64(f[3]);
        const auto pterm = parse_u64(f[4]);
        const auto commit = parse_u64(f[5]);
        if (!term || !leader || !pidx || !pterm || !commit) return std::nullopt;
        return AppendEntries{*term, *leader, *pidx, *pterm, *commit};
    }
    if (tag == "AER" && f.size() == 3) {
        const auto term = parse_u64(f[1]);
        const auto ok = parse_bool(f[2]);
        if (!term || !ok) return std::nullopt;
        return AppendEntriesReply{*term, *ok};
    }
    return std::nullopt;
}

}  // namespace quorum::raft
