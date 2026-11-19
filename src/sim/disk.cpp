#include "quorum/sim/disk.h"

#include <set>

namespace quorum::sim {

SimDisk::SimDisk(std::uint64_t seed, SimDiskConfig config) : rng_(seed), config_(config) {}

void SimDisk::check_powered() const {
    if (!powered_) throw DiskCrash();
}

bool SimDisk::crash_now() {
    check_powered();
    if (!crash_countdown_) return false;
    if (*crash_countdown_ > 0) {
        --*crash_countdown_;
        return false;
    }
    return true;
}

void SimDisk::crash_mid_call() {
    ++stats_.mid_call_crashes;
    power_loss();
    throw DiskCrash();
}

SimDisk::Inode& SimDisk::inode(const std::string& name) {
    const auto it = names_.find(name);
    if (it == names_.end()) throw std::runtime_error("SimDisk: no such file: " + name);
    return inodes_.at(it->second);
}

void SimDisk::issue(Inode& f, Op op) {
    if (op.append) {
        f.current += op.data;
    } else {
        f.current.resize(op.size);
    }
    f.pending.push_back(std::move(op));
}

std::optional<std::string> SimDisk::read(const std::string& name) {
    check_powered();
    const auto it = names_.find(name);
    if (it == names_.end()) return std::nullopt;
    return inodes_.at(it->second).current;
}

void SimDisk::create(const std::string& name) {
    const bool crash = crash_now();
    if (const auto it = names_.find(name); it != names_.end()) {
        issue(inodes_.at(it->second), Op{false, {}, 0});
    } else {
        const std::uint64_t id = next_inode_++;
        inodes_[id];
        names_[name] = id;
    }
    if (crash) crash_mid_call();
}

void SimDisk::append(const std::string& name, std::string_view data) {
    const bool crash = crash_now();
    issue(inode(name), Op{true, std::string(data), 0});
    if (crash) crash_mid_call();
}

void SimDisk::truncate(const std::string& name, std::uint64_t size) {
    const bool crash = crash_now();
    Inode& f = inode(name);
    if (size > f.current.size()) throw std::runtime_error("SimDisk: truncate would grow " + name);
    issue(f, Op{false, {}, size});
    if (crash) crash_mid_call();
}

void SimDisk::fsync(const std::string& name) {
    const bool crash = crash_now();
    Inode& f = inode(name);
    if (crash) crash_mid_call();
    f.durable = f.current;
    f.pending.clear();
    ++stats_.fsyncs;
}

void SimDisk::rename(const std::string& from, const std::string& to) {
    const bool crash = crash_now();
    const auto it = names_.find(from);
    if (it == names_.end()) throw std::runtime_error("SimDisk: no such file: " + from);
    const std::uint64_t id = it->second;
    names_.erase(it);
    names_[to] = id;
    if (crash) crash_mid_call();
}

void SimDisk::remove(const std::string& name) {
    const bool crash = crash_now();
    names_.erase(name);
    if (crash) crash_mid_call();
}

void SimDisk::fsync_dir() {
    if (crash_now()) crash_mid_call();
    durable_names_ = names_;
    ++stats_.dir_fsyncs;
}

void SimDisk::power_loss() {
    if (!powered_) return;
    powered_ = false;
    crash_countdown_.reset();
    ++stats_.power_losses;

    names_ = durable_names_;
    std::set<std::uint64_t> live;
    for (const auto& [name, id] : names_) live.insert(id);
    for (auto it = inodes_.begin(); it != inodes_.end();) {
        if (!live.count(it->first)) {
            it = inodes_.erase(it);
            continue;
        }
        Inode& f = it->second;
        std::string data = f.durable;
        const std::size_t n = f.pending.size();
        const auto keep = static_cast<std::size_t>(n == 0 ? 0 : rng_.uniform(0, n));
        for (std::size_t i = 0; i < keep; ++i) {
            const Op& op = f.pending[i];
            if (op.append) {
                data += op.data;
            } else {
                data.resize(op.size);
            }
        }
        if (keep < n) {
            const Op& next = f.pending[keep];
            if (next.append && next.data.size() >= 2 && rng_.bernoulli(config_.torn_write)) {
                data += next.data.substr(0, rng_.uniform(1, next.data.size() - 1));
                ++stats_.torn_writes;
            }
        }
        stats_.unsynced_ops_kept += keep;
        stats_.unsynced_ops_lost += n - keep;
        f.durable = data;
        f.current = std::move(data);
        f.pending.clear();
        ++it;
    }
}

void SimDisk::power_on() {
    powered_ = true;
}

void SimDisk::flip_bit(const std::string& name, std::uint64_t byte, unsigned bit) {
    Inode& f = inode(name);
    if (byte >= f.current.size() || byte >= f.durable.size() || bit > 7)
        throw std::out_of_range("SimDisk::flip_bit: no such bit in " + name);
    const auto mask = static_cast<char>(1u << bit);
    f.current[byte] ^= mask;
    f.durable[byte] ^= mask;
}

std::size_t SimDisk::unsynced_ops(const std::string& name) const {
    const auto it = names_.find(name);
    return it == names_.end() ? 0 : inodes_.at(it->second).pending.size();
}

}  // namespace quorum::sim
