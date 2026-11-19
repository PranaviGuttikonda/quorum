#include "quorum/raft/messages.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>

namespace quorum::raft {

namespace {

template <class... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};

// Given: fields separated by exactly one space, some of them raw byte strings
//        whose length was given by the previous field.
// Approach: a cursor that requires a single ' ' before every field except the
//           first. Empty tokens, doubled or trailing spaces, and short byte
//           strings all fail.
class Reader {
public:
    explicit Reader(std::string_view s) : s_(s) {}

    std::optional<std::string_view> token() {
        if (!separator()) return std::nullopt;
        std::size_t end = s_.find(' ', pos_);
        if (end == std::string_view::npos) end = s_.size();
        if (end == pos_) return std::nullopt;
        const std::string_view t = s_.substr(pos_, end - pos_);
        pos_ = end;
        return t;
    }

    // Approach: from_chars accepts no sign, whitespace or prefix, so "+1",
    //           " 1" and "0x1" are all rejected. The whole token must be used.
    std::optional<std::uint64_t> u64() {
        const auto t = token();
        if (!t) return std::nullopt;
        std::uint64_t v = 0;
        const auto [ptr, ec] = std::from_chars(t->data(), t->data() + t->size(), v);
        if (ec != std::errc{} || ptr != t->data() + t->size()) return std::nullopt;
        return v;
    }

    std::optional<NodeId> node() {
        const auto v = u64();
        if (!v || *v > std::numeric_limits<NodeId>::max()) return std::nullopt;
        return static_cast<NodeId>(*v);
    }

    std::optional<bool> flag() {
        const auto t = token();
        if (t == "0") return false;
        if (t == "1") return true;
        return std::nullopt;
    }

    std::optional<std::string_view> bytes(std::uint64_t n) {
        if (!separator() || n > s_.size() - pos_) return std::nullopt;
        const std::string_view b = s_.substr(pos_, n);
        pos_ += n;
        return b;
    }

    std::size_t remaining() const { return s_.size() - pos_; }
    bool done() const { return pos_ == s_.size(); }

private:
    bool separator() {
        if (first_) {
            first_ = false;
            return true;
        }
        if (pos_ < s_.size() && s_[pos_] == ' ') {
            ++pos_;
            return true;
        }
        return false;
    }

    std::string_view s_;
    std::size_t pos_ = 0;
    bool first_ = true;
};

std::optional<RaftMessage> decode_ae(Reader& r) {
    AppendEntries m;
    const auto term = r.u64();
    const auto leader = r.node();
    const auto pidx = r.u64();
    const auto pterm = r.u64();
    const auto commit = r.u64();
    const auto n = r.u64();
    if (!term || !leader || !pidx || !pterm || !commit || !n) return std::nullopt;
    // Every entry takes at least 5 bytes (" 0 0 " for an empty command), so a
    // count that can't fit is rejected before anything is allocated.
    if (*n > r.remaining() / 5) return std::nullopt;
    m.term = *term;
    m.leader_id = *leader;
    m.prev_log_index = *pidx;
    m.prev_log_term = *pterm;
    m.leader_commit = *commit;
    m.entries.reserve(*n);
    for (std::uint64_t i = 0; i < *n; ++i) {
        const auto et = r.u64();
        const auto len = r.u64();
        if (!et || !len) return std::nullopt;
        const auto cmd = r.bytes(*len);
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
    std::string out;
    auto put = [&out](std::uint64_t v) {
        out += ' ';
        out += std::to_string(v);
    };
    std::visit(Overloaded{
                   [&](const RequestVote& m) {
                       out = "RV";
                       put(m.term);
                       put(m.candidate_id);
                       put(m.last_log_index);
                       put(m.last_log_term);
                   },
                   [&](const RequestVoteReply& m) {
                       out = "RVR";
                       put(m.term);
                       put(m.vote_granted ? 1 : 0);
                   },
                   [&](const AppendEntries& m) {
                       out = "AE";
                       put(m.term);
                       put(m.leader_id);
                       put(m.prev_log_index);
                       put(m.prev_log_term);
                       put(m.leader_commit);
                       put(m.entries.size());
                       for (const LogEntry& e : m.entries) {
                           put(e.term);
                           put(e.command.size());
                           out += ' ';
                           out += e.command;
                       }
                   },
                   [&](const AppendEntriesReply& m) {
                       out = "AER";
                       put(m.term);
                       put(m.success ? 1 : 0);
                       put(m.match_index);
                       put(m.last_log_index);
                   },
               },
               msg);
    return out;
}

std::optional<RaftMessage> decode(std::string_view payload) {
    Reader r(payload);
    const auto tag = r.token();
    if (!tag) return std::nullopt;

    std::optional<RaftMessage> out;
    if (*tag == "RV") {
        const auto term = r.u64();
        const auto cand = r.node();
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
