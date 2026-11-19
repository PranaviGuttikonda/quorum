#pragma once

// Given: Raft RPCs, KV commands and client messages all travel as strings,
//        and some fields are arbitrary bytes.
// Approach: one strict text format shared by every codec. Fields are
//           separated by exactly one space. Numbers are unsigned decimal. A
//           byte string is written as its length, a space and then the raw
//           bytes. The Reader rejects empty tokens, doubled or trailing
//           spaces, signs and prefixes, short byte strings, and leftover input
//           (checked via done()).

#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace quorum::codec {

class Writer {
public:
    explicit Writer(std::string tag) : out_(std::move(tag)) {}

    Writer& u64(std::uint64_t v) {
        out_ += ' ';
        out_ += std::to_string(v);
        return *this;
    }
    Writer& flag(bool b) { return u64(b ? 1 : 0); }
    Writer& bytes(std::string_view b) {
        u64(b.size());
        out_ += ' ';
        out_ += b;
        return *this;
    }
    // Writes 0 for nullopt, otherwise 1 followed by the bytes.
    Writer& optional_bytes(const std::optional<std::string>& b) {
        flag(b.has_value());
        if (b) bytes(*b);
        return *this;
    }
    std::string str() const { return out_; }

private:
    std::string out_;
};

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

    std::optional<std::uint32_t> u32() {
        const auto v = u64();
        if (!v || *v > std::numeric_limits<std::uint32_t>::max()) return std::nullopt;
        return static_cast<std::uint32_t>(*v);
    }

    std::optional<bool> flag() {
        const auto t = token();
        if (t == "0") return false;
        if (t == "1") return true;
        return std::nullopt;
    }

    // A length field followed by that many raw bytes.
    std::optional<std::string_view> bytes() {
        const auto n = u64();
        if (!n || !separator() || *n > s_.size() - pos_) return std::nullopt;
        const std::string_view b = s_.substr(pos_, *n);
        pos_ += *n;
        return b;
    }

    // Outer optional: parse failure. Inner optional: the encoded value.
    std::optional<std::optional<std::string>> optional_bytes() {
        const auto present = flag();
        if (!present) return std::nullopt;
        if (!*present) return std::optional<std::string>{};
        const auto b = bytes();
        if (!b) return std::nullopt;
        return std::optional<std::string>{std::string(*b)};
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

}  // namespace quorum::codec
