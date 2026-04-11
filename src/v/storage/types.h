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

#include "base/format_to.h"
#include "base/units.h"
#include "compaction/types.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/file_sanitizer_types.h"
#include "storage/fwd.h"
#include "storage/logger.h"
#include "storage/scoped_file_tracker.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/file.hh> //io_priority
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <vector>

namespace storage {
using log_clock = ss::lowres_clock;
using jitter_percents = named_type<int, struct jitter_percents_tag>;

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
    // hints stm_hookset that now it's a good time to make a snapshot
    virtual void write_local_snapshot_in_background() = 0;

    // Lets the STM control snapshotting and local data removal by limiting
    // local log eviction and compaction attempts to offsets not greater than
    // this.
    //
    // For example, this can be used to ensure local data is not removed before
    // first uploading it to tiered storage.
    virtual model::offset max_removable_local_log_offset() = 0;

    // Lets the STM limit application-facing removal or compaction of data by
    // limiting GC or compaction attempts to offsets exclusively below this
    // offset. For data that is in both tiered storage and local storage, the
    // pin will not prevent removal of the local data because applications will
    // still see the data from tiered storage.
    //
    // For example, this can be used to ensure data is written to Iceberg
    // before being removed from the Kafka-application-facing log.
    //
    // May refer to a Kafka offset that does not exist, i.e. it may be up to or
    // equal to the high watermark.
    virtual std::optional<kafka::offset> lowest_pinned_data_offset() const = 0;

    virtual model::offset last_locally_snapshotted_offset() const = 0;

    virtual model::offset last_applied() const = 0;

    virtual const ss::sstring& name() = 0;

    // Only valid for state machines maintaining transactional state.
    // Returns aborted transactions in range [from, to] offsets.
    virtual ss::future<chunked_vector<model::tx_range>>
      aborted_tx_ranges(model::offset, model::offset) = 0;

    virtual model::control_record_type
    parse_tx_control_batch(const model::record_batch&) {
        return model::control_record_type::unknown;
    }

    virtual bool
    is_batch_in_idempotent_window(const model::record_batch_header&) const {
        return false;
    }
};

/**
 * stm_hookset is an interface responsible for the coordination of state
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
 * We pass stm_hookset to log.h and eviction_stm.h to give them a way to make
 * sure that a snapshot with given or newer offset exists before removing the
 * prior events. When a snapshot doesn't exist stm_hookset makes the snapshot
 * and returns control flow.
 *
 * make_snapshot lets log to hint when it's good time to make a snapshot e.g.
 * after a segment roll. It's up to a state machine to decide whether to make
 * it now or on its own pace.
 */
class stm_hookset {
public:
    ~stm_hookset() {
        vassert(
          _status != status::running,
          "stm_hookset is destructed without shutting down");
    }

    void start() {
        vassert(
          _status == status::not_started, "Invalid status transition on start");
        _status = status::running;
        _status_running_cv.broadcast();
    }

    void stop() {
        vassert(
          _status != status::shutting_down,
          "Invalid status transition on stop");
        if (_status == status::not_started) {
            _status_running_cv.broadcast();
        }
        _status = status::shutting_down;
    }

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
        auto f = await_start();
        for (auto stm : _stms) {
            f = f.then([stm, offset]() {
                return stm->ensure_local_snapshot_exists(offset);
            });
        }
        return f;
    }

    bool request_make_snapshot_in_background() {
        if (_status != status::running) {
            vlog(
              stlog.warn,
              "request to make snapshot in background is ignored because stm "
              "manager is not running");
            return false;
        }
        for (auto& stm : _stms) {
            stm->write_local_snapshot_in_background();
        }
        return true;
    }

    model::offset max_removable_local_log_offset();
    std::optional<kafka::offset> lowest_pinned_data_offset() const;
    model::offset max_tombstone_remove_offset() const;
    void set_max_tombstone_remove_offset(model::offset);
    model::offset max_tx_end_remove_offset() const;
    void set_max_tx_end_remove_offset(model::offset);

    ss::future<chunked_vector<model::tx_range>>
    aborted_tx_ranges(model::offset to, model::offset from) {
        co_await await_start();
        chunked_vector<model::tx_range> r;
        if (_tx_stm) {
            r = co_await _tx_stm->aborted_tx_ranges(to, from);
        }
        co_return r;
    }

    model::control_record_type
    parse_tx_control_batch(const model::record_batch& b) {
        check_status("parse_tx_control_batch");
        if (!_tx_stm) {
            return model::control_record_type::unknown;
        }
        return _tx_stm->parse_tx_control_batch(b);
    }

    const std::vector<ss::shared_ptr<snapshotable_stm>>& stms() const {
        check_status("stms");
        return _stms;
    }

    std::optional<storage::stm_type> transactional_stm_type() const {
        check_status("transactional_stm_type");
        if (_tx_stm) {
            return _tx_stm->type();
        }
        return std::nullopt;
    }

    const ss::shared_ptr<snapshotable_stm> transactional_stm() const {
        check_status("transactional_stm");
        return _tx_stm;
    }

    /**
     * Returns the offset of the last snapshot taken by the transactional state
     * machine. if no transactional stm is registered, returns max offset.
     */
    model::offset tx_snapshot_offset() const;

    bool is_batch_in_idempotent_window(const model::record_batch_header&) const;

private:
    void check_status(std::string_view operation) const {
        switch (_status) {
        case status::not_started: {
            vassert(
              false, "stm_hookset has not started for operation {}", operation);
        }
        case status::running:
            [[likely]] return;
        case status::shutting_down:
            throw ss::gate_closed_exception();
        }
    }

    bool has_started() const {
        if (_status == status::not_started) {
            return false;
        }
        check_status("has_started");
        return true;
    }

    ss::future<> await_start() const {
        if (_status == status::not_started) {
            vlog(
              stlog.error,
              "attempt to list STMs before stm_hookset is started");
            co_await _status_running_cv.wait();
        }
        check_status("await_start");
    }

    enum class status : uint8_t { not_started, running, shutting_down };

    ss::shared_ptr<snapshotable_stm> _tx_stm;
    std::vector<ss::shared_ptr<snapshotable_stm>> _stms;
    model::offset _max_tombstone_remove_offset{};
    model::offset _max_tx_end_remove_offset{};
    status _status{status::not_started};
    mutable ss::condition_variable _status_running_cv;
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

/// A timequery configuration specifies the range of offsets to search for a
/// record with a timestamp equal to or greater than the specified time.
struct timequery_config {
    timequery_config(
      model::offset min_offset,
      model::timestamp t,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      model::opt_abort_source_t as = std::nullopt,
      model::opt_client_address_t client_addr = std::nullopt) noexcept
      : min_offset(min_offset)
      , time(t)
      , max_offset(max_offset)
      , type_filter(type_filter)
      , abort_source(as)
      , client_address(std::move(client_addr)) {}
    model::offset min_offset;
    model::timestamp time;
    model::offset max_offset;
    std::optional<model::record_batch_type> type_filter;
    model::opt_abort_source_t abort_source;
    model::opt_client_address_t client_address;

    friend std::ostream& operator<<(std::ostream& o, const timequery_config&);
};
struct timequery_result {
    timequery_result(
      model::term_id term, model::offset o, model::timestamp t) noexcept
      : term(term)
      , offset(o)
      , time(t) {}

    model::term_id term;
    model::offset offset;
    model::timestamp time;

    bool operator==(const timequery_result& other) const = default;

    friend std::ostream& operator<<(std::ostream& o, const timequery_result&);
};

struct truncate_config {
    truncate_config(model::offset o)
      : base_offset(o) {}
    // Lowest offset to remove.
    model::offset base_offset;
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
      std::optional<model::offset_delta> force_truncate_delta = std::nullopt)
      : start_offset(o)
      , force_truncate_delta(force_truncate_delta) {}
    model::offset start_offset;

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

/**
 * Log reader configuration. Operates on Raft offsets.
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
struct local_log_reader_config {
    local_log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      size_t max_bytes,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time,
      model::opt_abort_source_t as,
      model::opt_client_address_t client_addr = std::nullopt,
      bool strict_max_bytes = false)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , max_bytes(max_bytes)
      , type_filter(type_filter)
      , timestamp(time)
      , abort_source(as)
      , client_address(std::move(client_addr))
      , strict_max_bytes(strict_max_bytes) {}

    /**
     * Read offsets [start, end].
     */
    local_log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      model::opt_abort_source_t as = std::nullopt,
      model::opt_client_address_t client_addr = std::nullopt)
      : local_log_reader_config(
          start_offset,
          max_offset,
          std::numeric_limits<size_t>::max(),
          std::nullopt,
          std::nullopt,
          as,
          std::move(client_addr),
          false) {}

    model::offset start_offset;
    model::offset max_offset;
    size_t max_bytes;

    // Batch type to filter for (i.e specified type will be the only one
    // observed in read).
    std::optional<model::record_batch_type> type_filter;

    /// For timequeries: this timestamp allows us to skip all such batches where
    /// `timestamp > batch.header().max_timestamp`.
    std::optional<model::timestamp> timestamp;

    /// abort source for read operations
    model::opt_abort_source_t abort_source;

    model::opt_client_address_t client_address;

    // Tracks number of consumed bytes in lower level readers
    size_t bytes_consumed{0};

    // Used to signal to the log reader that consumed bytes has exceeded
    // max_bytes and that reading should stop
    bool over_budget{false};

    // do not let the lower level readers go over budget even when that means
    // that the reader will return no batches.
    bool strict_max_bytes{false};

    // allow cache reads, but skip lru promotion and cache insertions on miss.
    // use this option when a reader shouldn't perturb the cache (e.g.
    // historical read-once workloads like compaction).
    bool skip_batch_cache{false};

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
    model::translate_offsets translate_offsets{false};

    // Timeout for segment range lock acquisition
    std::optional<ss::semaphore::clock::time_point> read_lock_deadline{};

    fmt::iterator format_to(fmt::iterator it) const;

    // The amount of data accumulated when reading from a segment before
    // returning results to the reader.
    static constexpr size_t segment_reader_max_buffer_size{32_KiB};
};

// Empty, invalid reader config which is sometimes useful as a placeholder
// since local_log_reader_config doesn't have a default constructor.
inline const local_log_reader_config empty_local_reader_config{{}, {}};

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
      model::offset max_tombstone_remove_offset,
      model::offset max_tx_end_remove_offset,
      std::optional<std::chrono::milliseconds> tombstone_retention_ms,
      std::optional<std::chrono::milliseconds> tx_retention_ms,
      std::chrono::milliseconds min_lag_ms,
      ss::abort_source& as,
      std::optional<ntp_sanitizer_config> san_cfg = std::nullopt,
      compaction::hash_key_offset_map* key_map = nullptr)
      : compact(
          max_collect_offset,
          max_tombstone_remove_offset,
          max_tx_end_remove_offset,
          tombstone_retention_ms,
          tx_retention_ms,
          as,
          std::move(san_cfg),
          std::nullopt,
          min_lag_ms,
          key_map)
      , gc(upper, max_bytes_in_log) {}

    compaction::compaction_config compact;
    gc_config gc;

    static housekeeping_config make_config(
      model::timestamp upper,
      std::optional<size_t> max_bytes_in_log,
      model::offset max_collect_offset,
      model::offset max_tombstone_remove_offset,
      model::offset max_tx_end_remove_offset,
      std::optional<std::chrono::milliseconds> tombstone_retention_ms,
      std::optional<std::chrono::milliseconds> tx_retention_ms,
      std::chrono::milliseconds min_lag_ms,
      ss::abort_source& as,
      std::optional<ntp_sanitizer_config> san_cfg = std::nullopt,
      compaction::hash_key_offset_map* key_map = nullptr) {
        auto cfg = housekeeping_config(
          upper,
          max_bytes_in_log,
          max_collect_offset,
          max_tombstone_remove_offset,
          max_tx_end_remove_offset,
          tombstone_retention_ms,
          tx_retention_ms,
          min_lag_ms,
          as,
          san_cfg,
          key_map);
        return cfg;
    }

    friend std::ostream& operator<<(std::ostream&, const housekeeping_config&);
};

struct compaction_result {
    explicit compaction_result(size_t sz)
      : executed_compaction(false)
      , size_before(sz)
      , size_after(sz) {}

    compaction_result(
      size_t before,
      size_t after,
      std::optional<size_t> cmp_idx_size_after = std::nullopt)
      : executed_compaction(true)
      , size_before(before)
      , size_after(after)
      , cmp_idx_size_after(cmp_idx_size_after) {}

    bool did_compact() const { return executed_compaction; }

    double compaction_ratio() const {
        return static_cast<double>(size_after)
               / static_cast<double>(size_before);
    }

    bool executed_compaction;
    size_t size_before;
    size_t size_after;
    // The size of the new compacted index, if one was made.
    std::optional<size_t> cmp_idx_size_after{std::nullopt};
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
