/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/ntp_config.h"
#include "tristate.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh> //io_priority
#include <seastar/core/rwlock.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <vector>

namespace storage {
/**
 * ghost record batch type, used by raft recovery to deliver gapless stream of
 * batches to follower, ghost batches aren't appended to the log
 */
constexpr model::record_batch_type ghost_record_batch_type
  = model::well_known_record_batch_types[7];

using log_clock = ss::lowres_clock;
using debug_sanitize_files = ss::bool_class<struct debug_sanitize_files_tag>;

class snapshotable_stm {
public:
    // create a snapshot at given offset unless a snapshot with given or newer
    // offset already exists
    virtual ss::future<> ensure_snapshot_exists(model::offset) = 0;
    // hints stm_manager that now it's a good time to make a snapshot
    virtual ss::future<> make_snapshot() = 0;
};

/**
 * stm_manager is an interface responsible for the coordination of state
 * machines  snapshotting and the log operations such as eviction and
 * compaction. We use  snapshots for two purposes:
 *
 *   - To make the start up faster by reading the last snapshot and catching up
 *     with events written after the snapshot was made instead of reading the
 *     whole log from the beginning.
 *
 *   - To reduce storage consumption by storing only a state and by removing
 *     the history of the events leading to the state.
 *
 * However when the snapshotting and log removal isn't synchronised it may lead
 * to data loss. Let a log eviction happens before snapshotting then it leads
 * to a situation when the state exists only in RAM which makes the system
 * vulnerable to power outages.
 *
 * We pass stm_manager to log.h and eviction_stm.h to give them a way to make
 * sure that a snapshot with given or newer offset exists before removing the
 * prior events. When a snapshot doesn't exist stm_manager makes the snapshot
 * and returns control flow.
 *
 * make_snapshot lets log to hint when it's good time to make a snapshot e.g.
 * after a segment roll. It's up to a state machine to decide whether to make
 * it now or on its own pace.
 */
class stm_manager {
public:
    void add_stm(ss::shared_ptr<snapshotable_stm> stm) { _stms.push_back(stm); }

    ss::future<> ensure_snapshot_exists(model::offset offset) {
        auto f = ss::now();
        for (auto stm : _stms) {
            f = f.then(
              [stm, offset]() { return stm->ensure_snapshot_exists(offset); });
        }
        return f;
    }

    ss::future<> make_snapshot() {
        auto f = ss::now();
        for (auto stm : _stms) {
            f = f.then([stm]() { return stm->make_snapshot(); });
        }
        return f;
    }

private:
    std::vector<ss::shared_ptr<snapshotable_stm>> _stms;
};

/// returns base_offset's from batches. Not max_offsets
struct offset_stats {
    model::offset start_offset;

    model::offset committed_offset;
    model::term_id committed_offset_term;

    model::offset dirty_offset;
    model::term_id dirty_offset_term;
    // Base offset of the first batch in the most recent term stored in log
    //
    // The last_term_base offset will be used by the followers to inform the
    // raft group leader about the next possible index where truncation should
    // happen while recovering node log.
    model::offset last_term_start_offset;

    friend std::ostream& operator<<(std::ostream&, const offset_stats&);
};

struct log_append_config {
    using fsync = ss::bool_class<class skip_tag>;
    fsync should_fsync;
    ss::io_priority_class io_priority;
    model::timeout_clock::time_point timeout;
};
struct append_result {
    log_clock::time_point append_time;
    model::offset base_offset;
    model::offset last_offset;
    size_t byte_size;
    model::term_id last_term;

    friend std::ostream& operator<<(std::ostream& o, const append_result&);
};

using opt_abort_source_t
  = std::optional<std::reference_wrapper<ss::abort_source>>;

struct timequery_config {
    timequery_config(
      model::timestamp t,
      model::offset o,
      ss::io_priority_class iop,
      opt_abort_source_t as = std::nullopt) noexcept
      : time(t)
      , max_offset(o)
      , prio(iop)
      , abort_source(as) {}
    model::timestamp time;
    model::offset max_offset;
    ss::io_priority_class prio;
    opt_abort_source_t abort_source;

    friend std::ostream& operator<<(std::ostream& o, const timequery_config&);
};
struct timequery_result {
    timequery_result(model::offset o, model::timestamp t) noexcept
      : offset(o)
      , time(t) {}
    model::offset offset;
    model::timestamp time;

    friend std::ostream& operator<<(std::ostream& o, const timequery_result&);
};

struct truncate_config {
    truncate_config(model::offset o, ss::io_priority_class p)
      : base_offset(o)
      , prio(p) {}
    model::offset base_offset;
    ss::io_priority_class prio;
    friend std::ostream& operator<<(std::ostream&, const truncate_config&);
};

/**
 * Prefix truncation configuration.
 *
 * Set start_offset to be the new starting offset of the log. It is required to
 * be a base offset of a record batch if that particular offset is contained in
 * the new log offset range. All offsets below the starting offset become
 * eligible for garbage collection, and are longer be accessible. If the
 * starting offset is contained in the log is not removed.
 */
struct truncate_prefix_config {
    truncate_prefix_config(model::offset o, ss::io_priority_class p)
      : start_offset(o)
      , prio(p) {}
    model::offset start_offset;
    ss::io_priority_class prio;

    friend std::ostream&
    operator<<(std::ostream&, const truncate_prefix_config&);
};

/**
 * Log reader configuration.
 *
 * The default reader configuration will read all batch types. To filter batches
 * by type add the types of interest to the type_filter set.
 *
 * The type filter is sorted before a segment scan, and a linear search is
 * performed. This will generally perform better than something like a binary
 * search when the size of the filter set is small (e.g. < 5). If you need to
 * use a larger filter then this design should be revisited.
 *
 * Start and max offset are inclusive.
 */
struct log_reader_config {
    model::offset start_offset;
    model::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;
    ss::io_priority_class prio;
    std::optional<model::record_batch_type> type_filter;

    /// \brief gurantees first_timestamp >= record_batch.first_timestamp
    /// it is the std::lower_bound
    std::optional<model::timestamp> first_timestamp;

    /// abort source for read operations
    opt_abort_source_t abort_source;

    // used by log reader
    size_t bytes_consumed{0};

    // skipping consumer sets this bit if a read would cause max_bytes to be
    // violated.  its used to signal to the log reader that even though consumed
    // bytes hasn't reached max bytes that reading should still stop.
    bool over_budget{false};

    // do not let the reader go over budget even when that means that the reader
    // will return no batches.
    bool strict_max_bytes{false};

    // allow cache reads, but skip lru promotion and cache insertions on miss.
    // use this option when a reader shouldn't perturb the cache (e.g.
    // historical read-once workloads like compaction).
    bool skip_batch_cache{false};

    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      ss::io_priority_class prio,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time,
      opt_abort_source_t as)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , prio(prio)
      , type_filter(type_filter)
      , first_timestamp(time)
      , abort_source(as) {}

    /**
     * Read offsets [start, end].
     */
    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      ss::io_priority_class prio,
      opt_abort_source_t as = std::nullopt)
      : log_reader_config(
        start_offset,
        max_offset,
        0,
        std::numeric_limits<size_t>::max(),
        prio,
        std::nullopt,
        std::nullopt,
        as) {}

    friend std::ostream& operator<<(std::ostream& o, const log_reader_config&);
};

struct compaction_config {
    explicit compaction_config(
      model::timestamp upper,
      std::optional<size_t> max_bytes_in_log,
      ss::io_priority_class p,
      ss::abort_source& as,
      debug_sanitize_files should_sanitize = debug_sanitize_files::no)
      : eviction_time(upper)
      , max_bytes(max_bytes_in_log)
      , iopc(p)
      , sanitize(should_sanitize)
      , asrc(&as) {}

    // remove everything below eviction time
    model::timestamp eviction_time;
    // remove one segment if log is > max_bytes
    std::optional<size_t> max_bytes;
    // priority for all IO in compaction
    ss::io_priority_class iopc;
    // use proxy fileops with assertions
    debug_sanitize_files sanitize;
    // abort source for compaction task
    ss::abort_source* asrc;

    friend std::ostream& operator<<(std::ostream&, const compaction_config&);
};
} // namespace storage
