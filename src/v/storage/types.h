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

#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/file_sanitizer_types.h"
#include "storage/fwd.h"
#include "storage/key_offset_map.h"
#include "storage/scoped_file_tracker.h"
#include "utils/tristate.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh> //io_priority
#include <seastar/core/rwlock.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <vector>

namespace storage {
using log_clock = ss::lowres_clock;
using jitter_percents = named_type<int, struct jitter_percents_tag>;

enum class disk_space_alert { ok = 0, low_space = 1, degraded = 2 };

inline disk_space_alert max_severity(disk_space_alert a, disk_space_alert b) {
    return std::max(a, b);
}

inline std::ostream& operator<<(std::ostream& o, const disk_space_alert d) {
    switch (d) {
    case disk_space_alert::ok:
        o << "ok";
        break;
    case disk_space_alert::low_space:
        o << "low_space";
        break;
    case disk_space_alert::degraded:
        o << "degraded";
        break;
    }
    return o;
}

struct disk
  : serde::envelope<disk, serde::version<1>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free{0};
    uint64_t total{0};
    disk_space_alert alert{disk_space_alert::ok};

    auto serde_fields() { return std::tie(path, free, total, alert); }

    // this value is _not_ serialized, but having it in this structure is useful
    // for passing the filesystem id around as the structure is used internally
    // to represent a disk not only for marshalling data to disk/network.
    unsigned long int fsid;

    friend std::ostream& operator<<(std::ostream&, const disk&);
    friend bool operator==(const disk&, const disk&) = default;
};

// Helps to identify transactional stms in the registered list of stms.
// Avoids an ugly dynamic cast to the base class.
enum class stm_type : int8_t {
    user_topic_transactional = 0,
    non_transactional = 1,
    consumer_offsets_transactional = 2
};

class snapshotable_stm {
public:
    virtual ~snapshotable_stm() = default;

    virtual stm_type type() { return stm_type::non_transactional; }

    // create a snapshot at given offset unless a snapshot with given or newer
    // offset already exists
    virtual ss::future<> ensure_local_snapshot_exists(model::offset) = 0;
    // hints stm_manager that now it's a good time to make a snapshot
    virtual void write_local_snapshot_in_background() = 0;
    // lets the stm control snapshotting and log eviction by limiting
    // log eviction attempts to offsets not greater than this.
    virtual model::offset max_collectible_offset() = 0;

    virtual model::offset last_applied() const = 0;

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
        if (
          stm->type() == stm_type::user_topic_transactional
          || stm->type() == stm_type::consumer_offsets_transactional) {
            vassert(!_tx_stm, "Multiple transactional stms not allowed.");
            _tx_stm = stm;
        }
        _stms.push_back(stm);
    }

    ss::future<> ensure_snapshot_exists(model::offset offset) {
        auto f = ss::now();
        for (auto stm : _stms) {
            f = f.then([stm, offset]() {
                return stm->ensure_local_snapshot_exists(offset);
            });
        }
        return f;
    }

    void make_snapshot_in_background() {
        for (auto& stm : _stms) {
            stm->write_local_snapshot_in_background();
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

    const std::vector<ss::shared_ptr<snapshotable_stm>>& stms() const {
        return _stms;
    }

    std::optional<storage::stm_type> transactional_stm_type() const {
        if (_tx_stm) {
            return _tx_stm->type();
        }
        return std::nullopt;
    }

    const ss::shared_ptr<snapshotable_stm> transactional_stm() const {
        return _tx_stm;
    }

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

using opt_client_address_t = std::optional<model::client_address_t>;

/// A timequery configuration specifies the range of offsets to search for a
/// record with a timestamp equal to or greater than the specified time.
struct timequery_config {
    timequery_config(
      model::offset min_offset,
      model::timestamp t,
      model::offset max_offset,
      ss::io_priority_class iop,
      std::optional<model::record_batch_type> type_filter,
      opt_abort_source_t as = std::nullopt,
      opt_client_address_t client_addr = std::nullopt) noexcept
      : min_offset(min_offset)
      , time(t)
      , max_offset(max_offset)
      , prio(iop)
      , type_filter(type_filter)
      , abort_source(as)
      , client_address(std::move(client_addr)) {}
    model::offset min_offset;
    model::timestamp time;
    model::offset max_offset;
    ss::io_priority_class prio;
    std::optional<model::record_batch_type> type_filter;
    opt_abort_source_t abort_source;
    opt_client_address_t client_address;

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
    // Lowest offset to remove.
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
    truncate_prefix_config(
      model::offset o,
      ss::io_priority_class p,
      std::optional<model::offset_delta> force_truncate_delta = std::nullopt)
      : start_offset(o)
      , prio(p)
      , force_truncate_delta(force_truncate_delta) {}
    model::offset start_offset;
    ss::io_priority_class prio;

    // When supplied and `start_offset` is ahead of the log's end offset,
    // indicates that truncation should proceed and this delta should be the
    // delta at the start offset.
    //
    // When not supplied, truncation past the log's end offset will result in
    // an error.
    std::optional<model::offset_delta> force_truncate_delta;

    friend std::ostream&
    operator<<(std::ostream&, const truncate_prefix_config&);
};

using translate_offsets = ss::bool_class<struct translate_tag>;

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
 * Start and max offset are inclusive. Because the reader only looks at batch
 * headers the first batch may start before the start offset and the last batch
 * may end after the max offset.
 *
 * Consider the following case:
 *
 *         cfg = {start offset = 14, max offset = 17}
 *                    +                      +
 *                    v                      v
 *  //-------+-------------+------------+-------------+-------//
 *  \\...9   |   10...14   |   15..15   |  16.....22  |  23...\\
 *  //-------+-------------+------------+-------------+-------//
 *           ^                                        ^
 *           |                                        |
 * The reader will actually return whole batches: [10, 14], [15, 15], [16, 22].
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

    opt_client_address_t client_address;

    // do not reuse cached readers. if this field is set to true the make_reader
    // method will proceed with creating a new reader without checking the
    // readers cache.
    bool skip_readers_cache{false};

    // If set to true, when a gap is detected in the offset space, installs a
    // `ghost_batch` to fill the gap with the same term as the next batch.
    //
    // The assumption here is that the gap was created by compaction, and that
    // the corresponding ghost batches exactly fill the space, while preserving
    // terms boundaries.
    bool fill_gaps{false};

    // If set to true, the offsets returned will be translated from Redpanda
    // offset to its data offset, as dictated by the underlying log's offset
    // translator types.
    //
    // NOTE: the translation refers only to the returned batches, not to the
    // input min/max offset bounds. Callers are expected to account for inputs
    // separately.
    translate_offsets translate_offsets{false};

    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      ss::io_priority_class prio,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time,
      opt_abort_source_t as,
      opt_client_address_t client_addr = std::nullopt)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , prio(prio)
      , type_filter(type_filter)
      , first_timestamp(time)
      , abort_source(as)
      , client_address(std::move(client_addr)) {}

    /**
     * Read offsets [start, end].
     */
    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      ss::io_priority_class prio,
      opt_abort_source_t as = std::nullopt,
      opt_client_address_t client_addr = std::nullopt)
      : log_reader_config(
          start_offset,
          max_offset,
          0,
          std::numeric_limits<size_t>::max(),
          prio,
          std::nullopt,
          std::nullopt,
          as,
          std::move(client_addr)) {}

    friend std::ostream& operator<<(std::ostream& o, const log_reader_config&);
};

struct gc_config {
    gc_config(model::timestamp upper, std::optional<size_t> max_bytes_in_log)
      : eviction_time(upper)
      , max_bytes(max_bytes_in_log) {}

    // remove everything below eviction time
    model::timestamp eviction_time;
    // remove one segment if log is > max_bytes
    std::optional<size_t> max_bytes;

    friend std::ostream& operator<<(std::ostream&, const gc_config&);
};

struct compaction_config {
    compaction_config(
      model::offset max_collect_offset,
      std::optional<std::chrono::milliseconds> tombstone_ret_ms,
      ss::io_priority_class p,
      ss::abort_source& as,
      std::optional<ntp_sanitizer_config> san_cfg = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      hash_key_offset_map* key_map = nullptr,
      scoped_file_tracker::set_t* to_clean = nullptr)
      : max_collectible_offset(max_collect_offset)
      , tombstone_retention_ms(tombstone_ret_ms)
      , iopc(p)
      , sanitizer_config(std::move(san_cfg))
      , key_offset_map_max_keys(max_keys)
      , hash_key_map(key_map)
      , files_to_cleanup(to_clean)
      , asrc(&as) {}

    // Cannot delete or compact past this offset (i.e. for unresolved txn
    // records): that is, only offsets <= this may be compacted.
    model::offset max_collectible_offset;

    // The retention time for tombstones. Tombstone removal occurs only for
    // "clean" compacted segments past the tombstone deletion horizon timestamp,
    // which is a segment's clean_compact_timestamp + tombstone_retention_ms.
    // This means tombstones take at least two rounds of compaction to remove a
    // tombstone: at least one pass to make a segment clean, and another pass
    // some time after tombstone.retention.ms to remove tombstones.
    //
    // Tombstone removal is only supported for topics with remote writes
    // disabled. As a result, this field will only have a value for compaction
    // ran on non-archival topics.
    std::optional<std::chrono::milliseconds> tombstone_retention_ms;

    // priority for all IO in compaction
    ss::io_priority_class iopc;
    // use proxy fileops with assertions and/or failure injection
    std::optional<ntp_sanitizer_config> sanitizer_config;

    // Limit the number of keys stored by a compaction's key-offset map.
    std::optional<size_t> key_offset_map_max_keys;

    // Hash key-offset map to reuse across compactions.
    hash_key_offset_map* hash_key_map;

    // Set of intermediary files added by compactions that need to be removed,
    // e.g. because they were leftover from an aborted compaction.
    scoped_file_tracker::set_t* files_to_cleanup;

    // abort source for compaction task
    ss::abort_source* asrc;

    friend std::ostream& operator<<(std::ostream&, const compaction_config&);
};

/*
 * Compaction and garbage collection are two distinct processes with their own
 * configuration. However, the vast majority of the time they are invoked
 * together as a single operation in a form of periodic housekeeping. This
 * structure is a convenience wrapper around the two configs.
 */
struct housekeeping_config {
    housekeeping_config(
      model::timestamp upper,
      std::optional<size_t> max_bytes_in_log,
      model::offset max_collect_offset,
      std::optional<std::chrono::milliseconds> tombstone_retention_ms,
      ss::io_priority_class p,
      ss::abort_source& as,
      std::optional<ntp_sanitizer_config> san_cfg = std::nullopt,
      hash_key_offset_map* key_map = nullptr)
      : compact(
          max_collect_offset,
          tombstone_retention_ms,
          p,
          as,
          std::move(san_cfg),
          std::nullopt,
          key_map)
      , gc(upper, max_bytes_in_log) {}

    compaction_config compact;
    gc_config gc;

    friend std::ostream& operator<<(std::ostream&, const housekeeping_config&);
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
 *
 * local_retention: amount of data that can safely be reclaimed that is above
 *                  the local retention policy. this will be affected by if
 *                  local retention is advisory or not.
 */
struct reclaim_size_limits {
    size_t retention{0};
    size_t available{0};
    size_t local_retention{0};

    friend reclaim_size_limits
    operator+(reclaim_size_limits lhs, const reclaim_size_limits& rhs) {
        lhs.retention += rhs.retention;
        lhs.available += rhs.available;
        lhs.local_retention += rhs.local_retention;
        return lhs;
    }
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
 * disk usage targets
 *
 * min_capacity: minimum amount of storage capacity needed.
 * min_capacity_wanted: minimum amount needed to meet policy requirements.
 *
 * The minimum capacity is intended to capture the minimum amount of disk space
 * needed for the basic functionality. At a high-level this is expressed as a
 * minimum number of segments per partition. Formally it is the sum of S *
 * log.max_segment_size() for each managed log, where S is the value of the
 * configuration option storage_reserve_min_segments specifying the minimum
 * number of segments for which space should be reserved.
 *
 * The minimum capacity wanted is an estimate of the amount of storage capacity
 * needed to meet various configurable targets. This value is derived from
 * multiple sources and policies:
 *
 *    * size-based retention: the amount of space needed to meet size-based
 *    local retention policy, rounded up to the nearest segment size.
 *
 *    * time-based retention: attempts to extrapolate the capacity requirements
 *    by examining recently written data and the apparent rate at which it has
 *    been written.
 *
 *    * compaction: compacted topics are kept whole on local storage (ie not
 *    subject to truncation due to local retention policies). for compact,delete
 *    topic the retention policy (not local retention) is used to express how
 *    much capacity is wanted. for a compact-only topic `2 * current size` is
 *    reported.
 */
struct usage_target {
    size_t min_capacity{0};
    size_t min_capacity_wanted{0};

    friend usage_target operator+(usage_target lhs, const usage_target& rhs) {
        lhs.min_capacity += rhs.min_capacity;
        lhs.min_capacity_wanted += rhs.min_capacity_wanted;
        return lhs;
    }
};

/*
 * disk usage report
 *
 * usage: disk usage summary for log.
 * reclaim: disk uage reclaim summary for log.
 * targets: target disk usage statistics.
 */
struct usage_report {
    usage usage;
    reclaim_size_limits reclaim;
    usage_target target;

    usage_report() = default;

    usage_report(
      struct usage usage, reclaim_size_limits reclaim, usage_target target)
      : usage(usage)
      , reclaim(reclaim)
      , target(target) {}

    friend usage_report operator+(usage_report lhs, const usage_report& rhs) {
        lhs.usage = lhs.usage + rhs.usage;
        lhs.reclaim = lhs.reclaim + rhs.reclaim;
        lhs.target = lhs.target + rhs.target;
        return lhs;
    }
};

/*
 * A set of categorized offsets annotated with the amount of data
 * represented in the log up to the offset, along with any other metadata
 * necessary to make low-disk reclaim decisions.
 *
 * This data structure is co-designed with the implementation of the reclaim
 * policy in resource_mgmt/storage.cc where the definitive documentation for
 * this data structure is located.
 */
struct reclaimable_offsets {
    struct offset {
        model::offset offset;
        size_t size;
    };

    ss::chunked_fifo<offset> effective_local_retention;
    ss::chunked_fifo<offset> low_space_non_hinted;
    ss::chunked_fifo<offset> low_space_hinted;
    ss::chunked_fifo<offset> active_segment;
    std::optional<size_t> force_roll;
};

} // namespace storage
