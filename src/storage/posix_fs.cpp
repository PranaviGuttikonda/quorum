#include "quorum/storage/posix_fs.h"

#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace quorum::storage {

namespace {

[[noreturn]] void fail(const char* call, const std::string& path) {
    throw std::system_error(errno, std::generic_category(), std::string(call) + " " + path);
}

// Closes on scope exit. A failing close() after a successful fsync can't lose
// synced data, so its result is ignored.
struct Fd {
    int fd;
    ~Fd() {
        if (fd >= 0) ::close(fd);
    }
};

Fd open_or_throw(const std::string& path, int flags, mode_t mode = 0) {
    int fd;
    do {
        fd = ::open(path.c_str(), flags, mode);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) fail("open", path);
    return Fd{fd};
}

void sync_fd(int fd, const std::string& path) {
#ifdef F_FULLFSYNC
    if (::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
    if (::fsync(fd) != 0) fail("fsync", path);
}

}  // namespace

PosixFileSystem::PosixFileSystem(std::string dir) : dir_(std::move(dir)) {
    struct stat st{};
    if (::stat(dir_.c_str(), &st) != 0) fail("stat", dir_);
    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        fail("PosixFileSystem", dir_);
    }
}

std::string PosixFileSystem::path(const std::string& name) const {
    return dir_ + "/" + name;
}

std::optional<std::string> PosixFileSystem::read(const std::string& name) {
    const std::string p = path(name);
    int raw;
    do {
        raw = ::open(p.c_str(), O_RDONLY);
    } while (raw < 0 && errno == EINTR);
    if (raw < 0) {
        if (errno == ENOENT) return std::nullopt;
        fail("open", p);
    }
    const Fd f{raw};
    std::string out;
    char buf[1 << 16];
    for (;;) {
        const ssize_t n = ::read(f.fd, buf, sizeof buf);
        if (n < 0) {
            if (errno == EINTR) continue;
            fail("read", p);
        }
        if (n == 0) break;
        out.append(buf, static_cast<std::size_t>(n));
    }
    return out;
}

void PosixFileSystem::create(const std::string& name) {
    (void)open_or_throw(path(name), O_WRONLY | O_CREAT | O_TRUNC, 0644);
}

void PosixFileSystem::append(const std::string& name, std::string_view data) {
    const std::string p = path(name);
    const Fd f = open_or_throw(p, O_WRONLY | O_APPEND);
    while (!data.empty()) {
        const ssize_t n = ::write(f.fd, data.data(), data.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            fail("write", p);
        }
        data.remove_prefix(static_cast<std::size_t>(n));
    }
}

void PosixFileSystem::truncate(const std::string& name, std::uint64_t size) {
    const std::string p = path(name);
    struct stat st{};
    if (::stat(p.c_str(), &st) != 0) fail("stat", p);
    if (static_cast<std::uint64_t>(st.st_size) < size) {
        errno = EINVAL;
        fail("truncate (would grow)", p);
    }
    if (::truncate(p.c_str(), static_cast<off_t>(size)) != 0) fail("truncate", p);
}

void PosixFileSystem::fsync(const std::string& name) {
    const std::string p = path(name);
    const Fd f = open_or_throw(p, O_RDONLY);
    sync_fd(f.fd, p);
}

void PosixFileSystem::rename(const std::string& from, const std::string& to) {
    if (::rename(path(from).c_str(), path(to).c_str()) != 0) fail("rename", path(from));
}

void PosixFileSystem::remove(const std::string& name) {
    const std::string p = path(name);
    if (::unlink(p.c_str()) != 0 && errno != ENOENT) fail("unlink", p);
}

void PosixFileSystem::fsync_dir() {
    const Fd f = open_or_throw(dir_, O_RDONLY);
    sync_fd(f.fd, dir_);
}

}  // namespace quorum::storage
