/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "resource_mgmt/storage.h"
#include "storage/fwd.h"
#include "tristate.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh> //io_priority
#include <seastar/core/rwlock.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <vector>

namespace storage {
using log_clock = ss::lowres_clock;
using debug_sanitize_files = ss::bool_class<struct debug_sanitize_files_tag>;
using jitter_percents = named_type<int, struct jitter_percents_tag>;

struct disk
  : serde::envelope<disk, serde::version<1>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free{0};
    uint64_t total{0};
    disk_space_alert alert{disk_space_alert::ok};

    auto serde_fields() { return std::tie(path, free, total, alert); }

    friend std::ostream& operator<<(std::ostream&, const disk&);
    friend bool operator==(const disk&, const disk&) = default;
};

// Helps to identify transactional stms in the registered list of stms.
// Avoids an ugly dynamic cast to the base class.
enum class stm_type : int8_t { transactional = 0, non_transactional = 1 };

class snapshotable_stm {
public:
    virtual ~snapshotable_stm() = default;

    virtual stm_type type() { return stm_type::non_transactional; }

    // create a snapshot at given offset unless a snapshot with given or newer
    // offset already exists
    virtual ss::future<> ensure_snapshot_exists(model::offset) = 0;
    // hints stm_manager that now it's a good time to make a snapshot
    virtual void make_snapshot_in_background() = 0;
    // lets the stm control snapshotting and log eviction by limiting
    // log eviction attempts to offsets not greater than this.
    virtual model::offset max_collectible_offset() = 0;

    virtual const ss::sstring& name() = 0;

    // Only valid for state machines maintaining transactional state.
    // Returns aborted transactions in range [from, to] offsets.
    virtual ss::future<fragmented_vector<model::tx_range>>
      aborted_tx_ranges(model::offset, model::offset) = 0;

    virtual model::control_record_type
    parse_tx_control_batch(const model::record_batch&) {
        return model::control_record_type::unknown;
    }
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
    void add_stm(ss::shared_ptr<snapshotable_stm> stm) {
        if (stm->type() == stm_type::transactional) {
            vassert(!_tx_stm, "Multiple transactional stms not allowed.");
            _tx_stm = stm;
        }
        _stms.push_back(stm);
    }

    ss::future<> ensure_snapshot_exists(model::offset offset) {
        auto f = ss::now();
        for (auto stm : _stms) {
            f = f.then(
              [stm, offset]() { return stm->ensure_snapshot_exists(offset); });
        }
        return f;
    }

    void make_snapshot_in_background() {
        for (auto& stm : _stms) {
            stm->make_snapshot_in_background();
        }
    }

    model::offset max_collectible_offset();

    ss::future<fragmented_vector<model::tx_range>>
    aborted_tx_ranges(model::offset to, model::offset from) {
        fragmented_vector<model::tx_range> r;
        if (_tx_stm) {
            r = co_await _tx_stm->aborted_tx_ranges(to, from);
        }
        co_return r;
    }

    model::control_record_type
    parse_tx_control_batch(const model::record_batch& b) {
        if (!_tx_stm) {
            return model::control_record_type::unknown;
        }
        return _tx_stm->parse_tx_control_batch(b);
    }

    bool has_tx_stm() { return _tx_stm.get(); }

private:
    ss::shared_ptr<snapshotable_stm> _tx_stm;
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
      std::optional<model::record_batch_type> type_filter,
      opt_abort_source_t as = std::nullopt) noexcept
      : time(t)
      , max_offset(o)
      , prio(iop)
      , type_filter(type_filter)
      , abort_source(as) {}
    model::timestamp time;
    model::offset max_offset;
    ss::io_priority_class prio;
    std::optional<model::record_batch_type> type_filter;
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
      model::offset max_collect_offset,
      ss::io_priority_class p,
      ss::abort_source& as,
      debug_sanitize_files should_sanitize = debug_sanitize_files::no)
      : eviction_time(upper)
      , max_bytes(max_bytes_in_log)
      , max_collectible_offset(max_collect_offset)
      , iopc(p)
      , sanitize(should_sanitize)
      , asrc(&as) {}

    // remove everything below eviction time
    model::timestamp eviction_time;
    // remove one segment if log is > max_bytes
    std::optional<size_t> max_bytes;
    // Cannot delete or compact past this offset (i.e. for unresolved txn
    // records): that is, only offsets <= this may be compacted.
    model::offset max_collectible_offset;
    // priority for all IO in compaction
    ss::io_priority_class iopc;
    // use proxy fileops with assertions
    debug_sanitize_files sanitize;
    // abort source for compaction task
    ss::abort_source* asrc;

    friend std::ostream& operator<<(std::ostream&, const compaction_config&);
};

struct compaction_result {
    explicit compaction_result(size_t sz)
      : executed_compaction(false)
      , size_before(sz)
      , size_after(sz) {}

    compaction_result(size_t before, size_t after)
      : executed_compaction(true)
      , size_before(before)
      , size_after(after) {}

    bool did_compact() const { return executed_compaction; }

    double compaction_ratio() const {
        return static_cast<double>(size_after)
               / static_cast<double>(size_before);
    }

    bool executed_compaction;
    size_t size_before;
    size_t size_after;
    friend std::ostream& operator<<(std::ostream&, const compaction_result&);
};

/*
 * Report number of bytes available for reclaim under different scenarios.
 *
 * retention: number of bytes that would be reclaimed by applying
 *            the log's retention policy.
 *
 * available: number of bytes that could be safely reclaimed if the
 *            retention policy was ignored. for example reclaiming
 *            past the retention limits if the data being reclaimed
 *            has been uploaded to cloud storage.
 */
struct reclaim_size_limits {
    size_t retention{0};
    size_t available{0};
};

/*
 * disk usage
 *
 * data: segment data
 * index: offset/time index
 * compaction: compaction index
 */
struct usage {
    size_t data{0};
    size_t index{0};
    size_t compaction{0};

    size_t total() const { return data + index + compaction; }

    friend usage operator+(usage lhs, const usage& rhs) {
        lhs.data += rhs.data;
        lhs.index += rhs.index;
        lhs.compaction += rhs.compaction;
        return lhs;
    }
};

/*
 * disk usage report
 *
 * usage: disk usage summary for log.
 * reclaim: disk uage reclaim summary for log.
 */
struct usage_report {
    usage usage;
    reclaim_size_limits reclaim;
};

} // namespace storage
