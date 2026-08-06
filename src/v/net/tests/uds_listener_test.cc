// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "net/uds_path.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/test_tools.hpp>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>

// These tests exercise every net::uds_path entry point, which after the
// async rewrite drive Seastar's asynchronous file/socket interfaces
// (file_stat, file_accessible, file_exists, remove_file, chmod, connect).
// They run under SEASTAR_THREAD_TEST_CASE so the returned futures can be
// resolved with .get() on a Seastar thread. Coverage is table-driven:
// each table enumerates positive, negative, boundary, and corner cases for
// one function.

namespace {

/// Per-test scratch directory. Each test creates its own mkdtemp()
/// directory to avoid cross-test interference (tests may run in parallel
/// inside a single process when Seastar is configured that way).
class scratch_dir {
public:
    scratch_dir() {
        char tmpl[] = "/tmp/rp-uds-test-XXXXXX";
        const char* p = ::mkdtemp(tmpl);
        if (p == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        _dir = p;
    }
    scratch_dir(const scratch_dir&) = delete;
    scratch_dir& operator=(const scratch_dir&) = delete;
    ~scratch_dir() {
        std::error_code ec;
        std::filesystem::remove_all(_dir, ec);
    }
    const std::string& dir() const { return _dir; }
    std::string path(const std::string& name) const {
        return _dir + "/" + name;
    }

private:
    std::string _dir;
};

/// Bind + listen a plain AF_UNIX socket at `path` and return the listening
/// fd. Simulates a "live" peer for the stale-socket probe. Caller owns the
/// returned fd and must close() it.
int bind_listen_uds(const std::string& path) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    BOOST_REQUIRE(fd >= 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.c_str(), path.size());
    int rc = ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    BOOST_REQUIRE_MESSAGE(rc == 0, "bind failed: " << std::strerror(errno));
    rc = ::listen(fd, 1);
    BOOST_REQUIRE_MESSAGE(rc == 0, "listen failed: " << std::strerror(errno));
    return fd;
}

/// Bind an AF_UNIX socket at `path` but do NOT listen, then close the fd.
/// The socket inode remains on disk with no listener, so a connect probe
/// receives ECONNREFUSED — this is the "stale socket" a crashed broker
/// leaves behind.
void make_stale_socket(const std::string& path) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    BOOST_REQUIRE(fd >= 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.c_str(), path.size());
    BOOST_REQUIRE(
      ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    ::close(fd);
}

/// Create an empty regular file at `path`.
void make_regular_file(const std::string& path) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    BOOST_REQUIRE(fd >= 0);
    ::close(fd);
}

/// Run `f().get()` and report whether it threw. Keeps the table loops
/// declarative: each case states expect_ok and we compare.
template<typename F>
bool succeeds(F&& f) {
    try {
        std::forward<F>(f)();
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool path_exists(const std::string& path) {
    struct stat st{};
    // lstat: a symlink itself counts as "present" even if dangling.
    return ::lstat(path.c_str(), &st) == 0;
}

} // namespace

// ---------------------------------------------------------------------------
// chmod_uds_path: the full mode is applied, including the setuid/setgid/sticky
// bits (the documented setgid use case relies on this). nullopt => 0660.
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(chmod_uds_path_modes_table) {
    struct chmod_case {
        std::string_view name;
        std::optional<uint32_t> mode;
        uint32_t expected; // st_mode & 07777
    };
    const chmod_case cases[] = {
      {"explicit_0640", std::optional<uint32_t>{0640}, 0640u},
      {"default_when_nullopt", std::nullopt, 0660u},
      {"zero_mode_0000", std::optional<uint32_t>{0}, 0000u},
      {"world_rwx_0777", std::optional<uint32_t>{0777}, 0777u},
      {"setgid_02660", std::optional<uint32_t>{02660}, 02660u},
      {"sticky_01660", std::optional<uint32_t>{01660}, 01660u},
      {"all_special_bits_07777", std::optional<uint32_t>{07777}, 07777u},
    };
    for (const auto& c : cases) {
        scratch_dir s;
        std::string sock = s.path("rp.sock");
        int fd = bind_listen_uds(sock);
        net::chmod_uds_path(ss::sstring{sock}, c.mode).get();
        struct stat st{};
        BOOST_REQUIRE(::stat(sock.c_str(), &st) == 0);
        BOOST_CHECK_MESSAGE(
          (st.st_mode & 07777u) == c.expected,
          c.name << ": expected mode " << std::oct << c.expected << " got "
                 << (st.st_mode & 07777u) << std::dec);
        ::close(fd);
        ::unlink(sock.c_str());
    }
}

SEASTAR_THREAD_TEST_CASE(chmod_uds_path_missing_path_throws) {
    scratch_dir s;
    ss::sstring missing{s.path("does-not-exist.sock")};
    BOOST_CHECK_THROW(
      net::chmod_uds_path(missing, std::nullopt).get(), std::exception);
}

// ---------------------------------------------------------------------------
// verify_uds_bound: must accept only a socket we own; reject regular files,
// directories, fifos, and — the security-critical corner — a symlink at the
// path (stat is done with follow_symlink::no), plus a missing inode.
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(verify_uds_bound_table) {
    enum class kind {
        live_socket,
        regular_file,
        directory,
        fifo,
        symlink_to_socket,
        dangling_symlink,
        missing,
    };
    struct verify_case {
        std::string_view name;
        kind setup;
        bool expect_ok;
    };
    const verify_case cases[] = {
      {"socket_owned_by_us_ok", kind::live_socket, true},
      {"regular_file_rejected", kind::regular_file, false},
      {"directory_rejected", kind::directory, false},
      {"fifo_rejected", kind::fifo, false},
      {"symlink_to_socket_rejected", kind::symlink_to_socket, false},
      {"dangling_symlink_rejected", kind::dangling_symlink, false},
      {"missing_inode_rejected", kind::missing, false},
    };
    for (const auto& c : cases) {
        scratch_dir s;
        std::string path = s.path("rp.sock");
        int hold = -1;
        switch (c.setup) {
        case kind::live_socket:
            hold = bind_listen_uds(path);
            break;
        case kind::regular_file:
            make_regular_file(path);
            break;
        case kind::directory:
            BOOST_REQUIRE(::mkdir(path.c_str(), 0755) == 0);
            break;
        case kind::fifo:
            BOOST_REQUIRE(::mkfifo(path.c_str(), 0644) == 0);
            break;
        case kind::symlink_to_socket: {
            std::string real = s.path("real.sock");
            hold = bind_listen_uds(real);
            BOOST_REQUIRE(::symlink(real.c_str(), path.c_str()) == 0);
            break;
        }
        case kind::dangling_symlink:
            BOOST_REQUIRE(
              ::symlink(s.path("nonexistent").c_str(), path.c_str()) == 0);
            break;
        case kind::missing:
            break;
        }
        const bool ok = succeeds(
          [&] { net::verify_uds_bound(ss::sstring{path}).get(); });
        BOOST_CHECK_MESSAGE(
          ok == c.expect_ok,
          c.name << ": expected ok=" << c.expect_ok << " got ok=" << ok);
        if (hold >= 0) {
            ::close(hold);
        }
    }
}

// ---------------------------------------------------------------------------
// prepare_uds_path parent-directory validation: the parent must exist, be a
// directory (following symlinks), and be writable. A path with no parent
// component is rejected up front.
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(prepare_uds_path_parent_table) {
    enum class kind {
        valid_dir,
        symlink_to_dir,
        missing_parent,
        parent_is_regular_file,
        no_parent_component,
    };
    struct parent_case {
        std::string_view name;
        kind setup;
        bool expect_ok;
    };
    const parent_case cases[] = {
      {"valid_writable_dir_ok", kind::valid_dir, true},
      {"parent_symlink_to_dir_ok", kind::symlink_to_dir, true},
      {"missing_parent_rejected", kind::missing_parent, false},
      {"parent_is_regular_file_rejected", kind::parent_is_regular_file, false},
      {"no_parent_component_rejected", kind::no_parent_component, false},
    };
    for (const auto& c : cases) {
        scratch_dir s;
        std::string path;
        switch (c.setup) {
        case kind::valid_dir:
            path = s.path("rp.sock");
            break;
        case kind::symlink_to_dir: {
            std::string linkdir = s.path("linkdir");
            BOOST_REQUIRE(::symlink(s.dir().c_str(), linkdir.c_str()) == 0);
            path = linkdir + "/rp.sock";
            break;
        }
        case kind::missing_parent:
            path = s.path("no_such_dir") + "/rp.sock";
            break;
        case kind::parent_is_regular_file: {
            std::string f = s.path("afile");
            make_regular_file(f);
            path = f + "/rp.sock";
            break;
        }
        case kind::no_parent_component:
            // A bare relative name has an empty parent_path().
            path = "rp.sock";
            break;
        }
        const bool ok = succeeds(
          [&] { net::prepare_uds_path(ss::sstring{path}).get(); });
        BOOST_CHECK_MESSAGE(
          ok == c.expect_ok,
          c.name << ": expected ok=" << c.expect_ok << " got ok=" << ok);
    }
}

// ---------------------------------------------------------------------------
// prepare_uds_path target-inode handling: a fresh path and a *stale* socket
// succeed (the stale inode is unlinked); a live socket, a regular file, a
// directory, and a fifo are all rejected and left in place (prepare never
// unlinks a non-stale-socket inode).
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(prepare_uds_path_target_table) {
    enum class kind {
        fresh,
        stale_socket,
        live_socket,
        regular_file,
        directory,
        fifo,
    };
    struct target_case {
        std::string_view name;
        kind setup;
        bool expect_ok;
        bool expect_target_present_after;
    };
    const target_case cases[] = {
      {"fresh_path_ok", kind::fresh, true, false},
      {"stale_socket_unlinked_ok", kind::stale_socket, true, false},
      {"live_socket_rejected_kept", kind::live_socket, false, true},
      {"regular_file_rejected_kept", kind::regular_file, false, true},
      {"directory_rejected_kept", kind::directory, false, true},
      {"fifo_rejected_kept", kind::fifo, false, true},
    };
    for (const auto& c : cases) {
        scratch_dir s;
        std::string path = s.path("rp.sock");
        int hold = -1;
        switch (c.setup) {
        case kind::fresh:
            break;
        case kind::stale_socket:
            make_stale_socket(path);
            break;
        case kind::live_socket:
            hold = bind_listen_uds(path);
            break;
        case kind::regular_file:
            make_regular_file(path);
            break;
        case kind::directory:
            BOOST_REQUIRE(::mkdir(path.c_str(), 0755) == 0);
            break;
        case kind::fifo:
            BOOST_REQUIRE(::mkfifo(path.c_str(), 0644) == 0);
            break;
        }
        const bool ok = succeeds(
          [&] { net::prepare_uds_path(ss::sstring{path}).get(); });
        BOOST_CHECK_MESSAGE(
          ok == c.expect_ok,
          c.name << ": expected ok=" << c.expect_ok << " got ok=" << ok);
        BOOST_CHECK_MESSAGE(
          path_exists(path) == c.expect_target_present_after,
          c.name << ": expected target present="
                 << c.expect_target_present_after
                 << " got " << path_exists(path));
        if (c.expect_ok) {
            // On success the sibling advisory lockfile must have been created.
            BOOST_CHECK_MESSAGE(
              path_exists(path + ".lock"), c.name << ": lockfile missing");
        }
        if (hold >= 0) {
            ::close(hold);
        }
    }
}

// Corner case: the advisory lock is held for the lifetime of the process, so
// a second prepare on the same path (a distinct open of the sibling .lock)
// contends and must fail with LOCK_NB.
SEASTAR_THREAD_TEST_CASE(prepare_uds_path_advisory_lock_contended) {
    scratch_dir s;
    ss::sstring path{s.path("rp.sock")};
    net::prepare_uds_path(path).get(); // acquires + intentionally leaks lock fd
    BOOST_CHECK_THROW(
      net::prepare_uds_path(path).get(), std::runtime_error);
}

// ---------------------------------------------------------------------------
// cleanup_uds_path: best-effort removal of both the socket and its sibling
// lockfile in every present/absent combination. It must never throw and must
// leave neither file behind.
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(cleanup_uds_path_table) {
    struct cleanup_case {
        std::string_view name;
        bool socket_present;
        bool lock_present;
    };
    const cleanup_case cases[] = {
      {"both_present", true, true},
      {"socket_only", true, false},
      {"lock_only", false, true},
      {"neither_present", false, false},
    };
    for (const auto& c : cases) {
        scratch_dir s;
        std::string path = s.path("rp.sock");
        std::string lock = path + ".lock";
        if (c.socket_present) {
            make_stale_socket(path);
        }
        if (c.lock_present) {
            make_regular_file(lock);
        }
        BOOST_CHECK_MESSAGE(
          succeeds([&] { net::cleanup_uds_path(ss::sstring{path}).get(); }),
          c.name << ": cleanup threw");
        BOOST_CHECK_MESSAGE(
          !path_exists(path), c.name << ": socket not removed");
        BOOST_CHECK_MESSAGE(
          !path_exists(lock), c.name << ": lockfile not removed");
    }
}

// ---------------------------------------------------------------------------
// Integration: end-to-end sanity that ss::unix_domain_addr works under
// Seastar and accepts a connection from a plain AF_UNIX client. This is the
// Seastar-side of what application_services.cc relies on and mirrors the
// asynchronous connect used by the stale-socket probe.
// ---------------------------------------------------------------------------
SEASTAR_THREAD_TEST_CASE(seastar_unix_domain_listen_roundtrip) {
    scratch_dir s;
    ss::sstring path{s.path("rp.sock")};
    auto addr = ss::socket_address(ss::unix_domain_addr(std::string{path}));
    ss::listen_options lo;
    lo.reuse_address = true;
    auto server = ss::engine().listen(addr, lo);

    // Client: plain blocking AF_UNIX connect.
    int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    BOOST_REQUIRE(client >= 0);
    sockaddr_un sun{};
    sun.sun_family = AF_UNIX;
    std::memcpy(sun.sun_path, path.c_str(), path.size());
    // Seastar accept() runs on the reactor; dispatch it and the connect
    // concurrently.
    auto accepted = server.accept();
    int rc = ::connect(client, reinterpret_cast<sockaddr*>(&sun), sizeof(sun));
    BOOST_REQUIRE_MESSAGE(rc == 0, "connect failed: " << std::strerror(errno));
    auto [connection, remoteaddr] = accepted.get();
    (void)remoteaddr;
    ::close(client);
    // Dropping `connection` and `server` via scope closes both endpoints.
    // scratch_dir destructor cleans up the socket inode.
}
