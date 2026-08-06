// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>

namespace net {

/// Prepare an AF_UNIX socket path for bind().
///
/// Performs the following steps on shard 0:
///  1. Validates the parent directory exists and is writable.
///  2. If `path` exists:
///      - If it is a socket, attempts a connect. On ECONNREFUSED the
///        socket is treated as stale and unlinked (a warning is logged).
///        On successful connect, throws — another broker is live.
///      - If it is not a socket, throws (never unlinks arbitrary files).
///  3. Acquires an advisory lock on `<path>.lock` via flock(2). The lock
///     fd is owned by this process for the lifetime of the listener; it
///     is released when the process exits.
///
/// The filesystem checks, the stale-socket probe, and the unlink use
/// Seastar's asynchronous I/O interfaces so the reactor is not blocked.
/// The final flock(2) step is synchronous: flock has no Seastar equivalent
/// and the lock must be held on a raw fd for the process lifetime.
///
/// The returned future fails with std::runtime_error (or a
/// std::filesystem::filesystem_error, which derives from it) on any
/// unrecoverable precondition failure.
ss::future<> prepare_uds_path(const ss::sstring& path);

/// Apply `chmod(path, mode)` asynchronously. Called post-listen on shard 0
/// after the socket inode has been created by bind(). The full mode is
/// applied, including the setuid/setgid/sticky bits. `mode` defaults to
/// 0660 if nullopt. The returned future fails on error.
ss::future<> chmod_uds_path(const ss::sstring& path, std::optional<uint32_t> mode);

/// Post-bind verification (defense-in-depth).
///
/// Re-stats `path` without following symlinks and asserts that the inode we
/// ended up with is:
///   - a socket, not a symlink / regular file / anything else;
///   - owned by the current effective UID.
///
/// This closes the TOCTOU window between `prepare_uds_path`'s stat/unlink
/// and the subsequent bind()+chmod() call: if a local attacker with write
/// access to the parent directory swapped the target inode for a symlink
/// or a file they own, the mismatch is caught here and the broker fails
/// to start instead of serving traffic on a surprise inode.
///
/// The returned future fails with std::runtime_error if the invariant is
/// violated.
ss::future<> verify_uds_bound(const ss::sstring& path);

/// Best-effort cleanup of a UDS path at graceful shutdown. Unlinks both
/// `path` and `<path>.lock`. ENOENT is ignored; all other errors are
/// logged but not propagated (shutdown must proceed), so the returned
/// future never fails.
ss::future<> cleanup_uds_path(const ss::sstring& path);

} // namespace net
