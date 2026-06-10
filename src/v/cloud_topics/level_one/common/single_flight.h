/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/as_future.hh>

#include <expected>
#include <filesystem>
#include <optional>
#include <type_traits>

namespace cloud_topics::l1 {

/// Per-shard single-flight coordinator: dedups concurrent operations
/// keyed by a path so only one "leader" caller runs the work and the
/// rest merge onto a shared future for the leader's outcome.
///
/// The optimization is opportunistic, so the map size is bounded.
/// Beyond max_entries, callers run their work uncoordinated. Whatever
/// operation the work performs should have its own concurrency control
/// mechanism downstream.
class single_flight {
public:
    /// Internal promise/future payload. nullopt on success.
    using outcome = std::optional<io::errc>;

    /// What a caller of run() observes.
    ///
    /// Success branch: bool -- true iff this caller joined an
    /// existing leader (did not run work itself). False in the leader
    /// and uncoordinated (at-capacity) cases.
    ///
    /// Error branch: the work's errc. Could be any of:
    ///  - a merger whose abort_source fired while waiting
    ///  - a merger whose leader failed
    ///  - a worker (leader or uncoordinated) that failed
    /// In any of these cases, callers should treat this result as failed work
    /// and take appropriate measures to retry, propagate the error, etc.
    using run_result = std::expected<bool, io::errc>;

    /// Default bound on concurrent distinct keys tracked per shard.
    static constexpr size_t default_max_entries = 4096;

    explicit single_flight(size_t max_entries = default_max_entries) noexcept
      : _max_entries(max_entries) {}

    /// Run work under single-flight coordination for key.
    ///
    /// - If no caller on this shard is currently running work for
    ///   key, become the leader: insert an entry, run work,
    ///   publish its outcome to any concurrent mergers, then erase
    ///   the entry. Returns success(false).
    /// - If another caller is already running work for key, await
    ///   the leader's outcome and return success(true).
    /// - If the map is at max_entries, run work uncoordinated (no
    ///   entry inserted, no mergers possible). Returns success(false).
    /// - On any work failure (leader's own, inherited by mergers, or
    ///   merger-side abort), returns unexpected(errc).
    ///
    /// If work's returned future fails with an exception, the leader
    /// publishes errc::file_io_error to any mergers (so they don't
    /// hang) and re-raises to its caller.
    ///
    /// work must be invocable as ss::future<outcome>(). It runs
    /// at most once per call to run.
    template<typename WorkFn>
    requires std::is_invocable_r_v<ss::future<outcome>, WorkFn>
    ss::future<run_result>
    run(std::filesystem::path key, ss::abort_source& as, WorkFn work);

    size_t in_flight() const noexcept { return _entries.size(); }
    size_t capacity() const noexcept { return _max_entries; }

private:
    /// Outcome of the synchronous lookup-or-insert step inside run.
    enum class join_kind { leader, merger, at_capacity };
    struct join_result {
        join_kind kind;
        /// Engaged iff kind == merger. ss::future is not default
        /// constructible, hence the optional wrapper.
        std::optional<ss::future<outcome>> merge_future;
    };

    /// Synchronous lookup-or-insert step.
    join_result
    join_or_lead(const std::filesystem::path& key, ss::abort_source& as);

    /// Publish an outcome to mergers waiting on key and erase the entry.
    /// Called only the leader fiber, exactly once per flight.
    void release_leader(const std::filesystem::path& key, outcome o) noexcept;

    chunked_hash_map<std::filesystem::path, ss::shared_promise<outcome>>
      _entries;
    size_t _max_entries;
};

template<typename WorkFn>
requires std::is_invocable_r_v<ss::future<single_flight::outcome>, WorkFn>
ss::future<single_flight::run_result> single_flight::run(
  std::filesystem::path key, ss::abort_source& as, WorkFn work) {
    auto j = join_or_lead(key, as);

    if (j.kind == join_kind::merger) {
        auto fut = co_await ss::coroutine::as_future(
          std::move(*j.merge_future));
        if (fut.failed()) {
            // The merger's own abort_source fired while waiting on
            // the leader. The leader will still publish its outcome
            // later; we just surface cloud_op_timeout to this caller.
            fut.ignore_ready_future();
            co_return std::unexpected(io::errc::cloud_op_timeout);
        }
        auto o = fut.get();
        if (o.has_value()) {
            co_return std::unexpected(*o);
        }
        co_return true; // success (merged onto an existing leader)
    }

    // Leader or at-capacity uncoordinated: this caller runs work.
    const bool is_leader = j.kind == join_kind::leader;
    outcome o = io::errc::file_io_error;
    std::exception_ptr work_exception;

    auto work_fut = co_await ss::coroutine::as_future(work());
    if (work_fut.failed()) {
        work_exception = work_fut.get_exception();
        // Leave the outcome at file_io_error so the leader still releases any
        // waiting mergers.
    } else {
        o = work_fut.get();
    }

    if (is_leader) {
        release_leader(key, o);
    }
    if (work_exception) {
        std::rethrow_exception(work_exception);
    }
    if (o.has_value()) {
        co_return std::unexpected(*o);
    }
    co_return false; // ran work, did not merge
}

} // namespace cloud_topics::l1
