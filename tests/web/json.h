#pragma once

// Given: the visualizer snapshot is JSON, and the tests must read it back
//        to compare it with the engine.
// Approach: a strict, minimal recursive-descent parser for what JSON needs
//           here: objects, arrays, strings (with escapes), non-negative
//           integers, non-negative decimals (kept as double), true, false
//           and null. Anything else throws.

#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace quorum::testing {

struct JsonValue;
using JsonObject = std::map<std::string, JsonValue>;
using JsonArray = std::vector<JsonValue>;

struct JsonValue {
    std::variant<std::nullptr_t, bool, std::uint64_t, double, std::string, std::shared_ptr<JsonArray>,
                 std::shared_ptr<JsonObject>>
        v;

    bool is_null() const { return std::holds_alternative<std::nullptr_t>(v); }
    bool boolean() const { return std::get<bool>(v); }
    std::uint64_t num() const { return std::get<std::uint64_t>(v); }
    double real() const {
        return std::holds_alternative<double>(v) ? std::get<double>(v) : static_cast<double>(num());
    }
    const std::string& str() const { return std::get<std::string>(v); }
    const JsonArray& arr() const { return *std::get<std::shared_ptr<JsonArray>>(v); }
    const JsonObject& obj() const { return *std::get<std::shared_ptr<JsonObject>>(v); }
    bool has(const std::string& k) const { return obj().count(k) > 0; }
    const JsonValue& operator[](const std::string& k) const {
        const auto it = obj().find(k);
        if (it == obj().end()) throw std::out_of_range("json: no key " + k);
        return it->second;
    }
    const JsonValue& operator[](std::size_t i) const { return arr().at(i); }
};

class JsonParser {
public:
    explicit JsonParser(std::string_view s) : s_(s) {}

    JsonValue parse() {
        JsonValue v = value();
        ws();
        if (pos_ != s_.size()) fail("trailing input");
        return v;
    }

private:
    [[noreturn]] void fail(const std::string& what) const {
        throw std::runtime_error("json: " + what + " at " + std::to_string(pos_));
    }
    void ws() {
        while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\n' || s_[pos_] == '\t' || s_[pos_] == '\r'))
            ++pos_;
    }
    char peek() {
        ws();
        if (pos_ >= s_.size()) fail("unexpected end");
        return s_[pos_];
    }
    void expect(char c) {
        if (peek() != c) fail(std::string("expected ") + c);
        ++pos_;
    }
    bool literal(std::string_view w) {
        if (s_.substr(pos_, w.size()) != w) return false;
        pos_ += w.size();
        return true;
    }
    JsonValue value() {
        const char c = peek();
        if (c == '{') return object();
        if (c == '[') return array();
        if (c == '"') return JsonValue{string()};
        if (literal("true")) return JsonValue{true};
        if (literal("false")) return JsonValue{false};
        if (literal("null")) return JsonValue{nullptr};
        if (c >= '0' && c <= '9') return number();
        fail("unexpected character");
    }
    JsonValue number() {
        std::uint64_t n = 0;
        const std::size_t start = pos_;
        while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') n = n * 10 + (s_[pos_++] - '0');
        if (s_[start] == '0' && pos_ - start > 1) fail("leading zero");
        if (pos_ < s_.size() && s_[pos_] == '.') {
            ++pos_;
            const std::size_t frac = pos_;
            while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
            if (pos_ == frac) fail("bad decimal");
            return JsonValue{std::stod(std::string(s_.substr(start, pos_ - start)))};
        }
        return JsonValue{n};
    }
    std::string string() {
        expect('"');
        std::string out;
        while (true) {
            if (pos_ >= s_.size()) fail("unterminated string");
            const char c = s_[pos_++];
            if (c == '"') return out;
            if (static_cast<unsigned char>(c) < 0x20) fail("control character in string");
            if (c != '\\') {
                out += c;
                continue;
            }
            if (pos_ >= s_.size()) fail("bad escape");
            const char e = s_[pos_++];
            switch (e) {
                case '"': out += '"'; break;
                case '\\': out += '\\'; break;
                case '/': out += '/'; break;
                case 'n': out += '\n'; break;
                case 'r': out += '\r'; break;
                case 't': out += '\t'; break;
                case 'u': {
                    if (pos_ + 4 > s_.size()) fail("bad \\u");
                    const unsigned cp = static_cast<unsigned>(std::stoul(std::string(s_.substr(pos_, 4)), nullptr, 16));
                    pos_ += 4;
                    if (cp >= 0x80) fail("only ASCII \\u escapes are supported");
                    out += static_cast<char>(cp);
                    break;
                }
                default: fail("bad escape");
            }
        }
    }
    JsonValue array() {
        expect('[');
        auto a = std::make_shared<JsonArray>();
        if (peek() == ']') {
            ++pos_;
            return JsonValue{a};
        }
        while (true) {
            a->push_back(value());
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            expect(']');
            return JsonValue{a};
        }
    }
    JsonValue object() {
        expect('{');
        auto o = std::make_shared<JsonObject>();
        if (peek() == '}') {
            ++pos_;
            return JsonValue{o};
        }
        while (true) {
            if (peek() != '"') fail("expected key");
            std::string k = string();
            expect(':');
            if (!o->emplace(k, value()).second) fail("duplicate key " + k);
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            expect('}');
            return JsonValue{o};
        }
    }

    std::string_view s_;
    std::size_t pos_ = 0;
};

inline JsonValue parse_json(std::string_view s) {
    return JsonParser(s).parse();
}

}  // namespace quorum::testing
