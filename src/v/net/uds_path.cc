// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/uds_path.h"

#include "base/seastarx.h"
#include "base/vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/unix_address.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace net {

namespace {

ss::logger udslog("net_uds");

[[noreturn]] void throw_errno(const std::string& op, const ss::sstring& path) {
    const int e = errno;
    throw std::runtime_error(
      fmt::format(
        "UDS {}: path='{}': {} (errno={})", op, path, std::strerror(e), e));
}

/// Returns true if a connect to `path` succeeds (some process is
/// listening), false on ECONNREFUSED (stale socket file), rethrows for
/// anything else. Uses Seastar's asynchronous connect so the reactor is
/// not blocked while the connection is attempted.
ss::future<bool> probe_connect(const ss::sstring& path) {
    if (path.size() >= sizeof(sockaddr_un{}.sun_path)) {
        throw std::runtime_error(
          fmt::format(
            "UDS probe_connect: path too long ({} bytes): '{}'",
            path.size(),
            path));
    }
    try {
        // The connected_socket is discarded immediately: a successful
        // connect is all we need to know a live peer is listening.
        co_await ss::connect(
          ss::socket_address(ss::unix_domain_addr(std::string(path))));
        co_return true;
    } catch (const std::system_error& e) {
        if (e.code() == std::errc::connection_refused) {
            co_return false;
        }
        throw;
    }
}

/// Best-effort unlink that ignores a missing file and only logs other
/// failures. Used by graceful shutdown, which must proceed regardless.
/// Takes `path` by value: it is invoked with a temporary and must own its
/// argument across the co_await.
ss::future<> best_effort_remove(ss::sstring path) {
    try {
        co_await ss::remove_file(path);
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() != std::errc::no_such_file_or_directory) {
            vlog(
              udslog.warn,
              "cleanup: remove_file('{}') failed: {}",
              path,
              e.what());
        }
    }
}

} // namespace

ss::future<> prepare_uds_path(const ss::sstring& path) {
    std::filesystem::path fspath{std::string{path}};
    auto parent = fspath.parent_path();
    if (parent.empty()) {
        throw std::runtime_error(
          fmt::format("UDS prepare: path '{}' has no parent directory", path));
    }
    ss::sstring parent_str{parent.string()};

    auto parent_st = co_await ss::file_stat(
      parent_str, ss::follow_symlink::yes);
    if (parent_st.type != ss::directory_entry_type::directory) {
        throw std::runtime_error(
          fmt::format(
            "UDS prepare: parent '{}' of '{}' is not a directory",
            parent.string(),
            path));
    }
    if (!co_await ss::file_accessible(parent_str, ss::access_flags::write)) {
        throw std::runtime_error(
          fmt::format(
            "UDS prepare: parent '{}' of '{}' is not writable",
            parent.string(),
            path));
    }

    if (co_await ss::file_exists(path)) {
        auto st = co_await ss::file_stat(path, ss::follow_symlink::yes);
        if (st.type != ss::directory_entry_type::socket) {
            throw std::runtime_error(
              fmt::format(
                "UDS prepare: path '{}' exists and is not a socket "
                "(mode={:#o}); refusing to unlink",
                path,
                st.mode));
        }
        // Existing socket: probe to decide whether it is stale.
        if (co_await probe_connect(path)) {
            throw std::runtime_error(
              fmt::format(
                "UDS prepare: path '{}' is a live socket — another broker "
                "appears to be listening",
                path));
        }
        vlog(
          udslog.warn,
          "Unlinking stale UDS socket at '{}' (connect probe refused)",
          path);
        try {
            co_await ss::remove_file(path);
        } catch (const std::filesystem::filesystem_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw;
            }
        }
    }

    // Advisory lock on <path>.lock. flock(2) has no Seastar asynchronous
    // equivalent and the lock must be held on a raw fd for the lifetime of
    // the process (the kernel releases it on exit), so this final step is
    // intentionally synchronous. The open()+flock() pair runs once, on
    // shard 0, during startup — not on a hot path. The lock fd is
    // intentionally leaked: its lifetime is the process.
    ss::sstring lock_path = path + ".lock";
    int lock_fd = ::open(
      lock_path.c_str(),
      O_RDWR | O_CREAT | O_CLOEXEC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (lock_fd < 0) {
        throw_errno("open(lockfile)", lock_path);
    }
    if (::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
        int e = errno;
        ::close(lock_fd);
        if (e == EWOULDBLOCK) {
            throw std::runtime_error(
              fmt::format(
                "UDS prepare: another process holds the advisory lock on "
                "'{}'",
                lock_path));
        }
        errno = e;
        throw_errno("flock", lock_path);
    }
}

ss::future<>
chmod_uds_path(const ss::sstring& path, std::optional<uint32_t> mode) {
    const mode_t m = mode.value_or(0660);
    // static_cast preserves the full mode, including the setuid/setgid/sticky
    // bits: Seastar's chmod passes the value straight through to ::chmod().
    co_await ss::chmod(path, static_cast<ss::file_permissions>(m));
}

ss::future<> verify_uds_bound(const ss::sstring& path) {
    // follow_symlink::no is the whole point of this check: detect if
    // something replaced our target inode with a symlink between
    // prepare_uds_path()'s stat/unlink and the subsequent bind().
    auto st = co_await ss::file_stat(path, ss::follow_symlink::no);
    if (st.type != ss::directory_entry_type::socket) {
        throw std::runtime_error(
          fmt::format(
            "UDS verify: path '{}' is not a socket after bind "
            "(mode={:#o}); possible symlink-race attack, refusing to start",
            path,
            st.mode));
    }
    const uint64_t me = ::geteuid();
    if (st.uid != me) {
        throw std::runtime_error(
          fmt::format(
            "UDS verify: path '{}' is owned by uid {} but we run as uid {}; "
            "possible inode-swap attack, refusing to start",
            path,
            st.uid,
            me));
    }
}

ss::future<> cleanup_uds_path(const ss::sstring& path) {
    co_await best_effort_remove(path);
    co_await best_effort_remove(path + ".lock");
}

} // namespace net
