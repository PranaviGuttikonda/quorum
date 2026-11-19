#include "quorum/kv/command.h"

#include <utility>

#include "quorum/codec/text.h"

namespace quorum::kv {

const char* to_string(Op op) {
    switch (op) {
        case Op::Put: return "put";
        case Op::Get: return "get";
        case Op::Delete: return "delete";
        case Op::Cas: return "cas";
    }
    return "?";
}

Command Command::put(ClientId c, Seq s, std::string k, std::string v) {
    return Command{c, s, Op::Put, std::move(k), std::move(v), std::nullopt};
}
Command Command::get(ClientId c, Seq s, std::string k) {
    return Command{c, s, Op::Get, std::move(k), {}, std::nullopt};
}
Command Command::del(ClientId c, Seq s, std::string k) {
    return Command{c, s, Op::Delete, std::move(k), {}, std::nullopt};
}
Command Command::cas(ClientId c, Seq s, std::string k, std::optional<std::string> expected,
                     std::string v) {
    return Command{c, s, Op::Cas, std::move(k), std::move(v), std::move(expected)};
}

std::string encode(const Command& c) {
    switch (c.op) {
        case Op::Put: return codec::Writer("PUT").u64(c.client).u64(c.seq).bytes(c.key).bytes(c.value).str();
        case Op::Get: return codec::Writer("GET").u64(c.client).u64(c.seq).bytes(c.key).str();
        case Op::Delete: return codec::Writer("DEL").u64(c.client).u64(c.seq).bytes(c.key).str();
        case Op::Cas:
            return codec::Writer("CAS")
                .u64(c.client)
                .u64(c.seq)
                .bytes(c.key)
                .optional_bytes(c.expected)
                .bytes(c.value)
                .str();
    }
    return {};
}

namespace {

// Reads a command from `r`, leaving any following input in place.
std::optional<Command> read_command(codec::Reader& r) {
    const auto tag = r.token();
    const auto client = r.u64();
    const auto seq = r.u64();
    const auto key = r.bytes();
    if (!tag || !client || !seq || !key || *seq == 0) return std::nullopt;
    Command c;
    c.client = *client;
    c.seq = *seq;
    c.key = std::string(*key);
    if (*tag == "PUT") {
        const auto v = r.bytes();
        if (!v) return std::nullopt;
        c.op = Op::Put;
        c.value = std::string(*v);
    } else if (*tag == "GET") {
        c.op = Op::Get;
    } else if (*tag == "DEL") {
        c.op = Op::Delete;
    } else if (*tag == "CAS") {
        const auto expected = r.optional_bytes();
        const auto v = r.bytes();
        if (!expected || !v) return std::nullopt;
        c.op = Op::Cas;
        c.expected = *expected;
        c.value = std::string(*v);
    } else {
        return std::nullopt;
    }
    return c;
}

}  // namespace

std::optional<Command> decode_command(std::string_view s) {
    codec::Reader r(s);
    auto c = read_command(r);
    if (!c || !r.done()) return std::nullopt;
    return c;
}

std::string encode_request(const Command& c) {
    return codec::Writer("KREQ").bytes(encode(c)).str();
}

std::optional<Command> decode_request(std::string_view s) {
    codec::Reader r(s);
    const auto tag = r.token();
    const auto inner = r.bytes();
    if (!tag || *tag != "KREQ" || !inner || !r.done()) return std::nullopt;
    return decode_command(*inner);
}

std::string encode(const Reply& rep) {
    codec::Writer w("KRESP");
    w.u64(rep.client).u64(rep.seq);
    if (const auto* res = std::get_if<Result>(&rep.body)) {
        w.u64(0).flag(res->ok).optional_bytes(res->value);  // 0 = OK
    } else {
        const auto& red = std::get<Redirect>(rep.body);
        w.u64(1).flag(red.leader_hint.has_value());  // 1 = not leader
        if (red.leader_hint) w.u64(*red.leader_hint);
    }
    return w.str();
}

std::optional<Reply> decode_reply(std::string_view s) {
    codec::Reader r(s);
    const auto tag = r.token();
    const auto client = r.u64();
    const auto seq = r.u64();
    const auto kind = r.u64();
    if (!tag || *tag != "KRESP" || !client || !seq || !kind) return std::nullopt;
    Reply rep{*client, *seq, Result{}};
    if (*kind == 0) {
        const auto ok = r.flag();
        const auto value = r.optional_bytes();
        if (!ok || !value) return std::nullopt;
        rep.body = Result{*ok, *value};
    } else if (*kind == 1) {
        const auto has = r.flag();
        if (!has) return std::nullopt;
        Redirect red;
        if (*has) {
            const auto hint = r.u32();
            if (!hint) return std::nullopt;
            red.leader_hint = *hint;
        }
        rep.body = red;
    } else {
        return std::nullopt;
    }
    if (!r.done()) return std::nullopt;
    return rep;
}

}  // namespace quorum::kv
