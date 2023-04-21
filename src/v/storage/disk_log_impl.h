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

#include "features/feature_table.h"
#include "model/fundamental.h"
#include "storage/disk_log_appender.h"
#include "storage/failure_probes.h"
#include "storage/lock_manager.h"
#include "storage/log.h"
#include "storage/log_reader.h"
#include "storage/probe.h"
#include "storage/readers_cache.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "utils/moving_average.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace storage {

class disk_log_impl final : public log::impl {
public:
    using failure_probes = storage::log_failure_probes;

    /*
     * the offset index can handle larger than 4gb segments. but the offset
     * index and compaction index still use 32-bit values to represent some
     * (relative) logical offsets which mean that a 4-billion limit on records
     * in a segment exists. by placing a hard limit on segment size we can avoid
     * those overflows because a segment with more than 4-billion records would
     * be larger than 4gb even when the records are empty. other practical but
     * not fundamental limits exist for large segment sizes like our current
     * in-memory representation of indices.
     *
     * the hard limit here is a slight mis-characterization. we apply this hard
     * limit to the user requested size. max segment size fuzzing is still
     * applied.
     */
    static constexpr size_t segment_size_hard_limit = 3_GiB;

    disk_log_impl(
      ntp_config,
      log_manager&,
      segment_set,
      kvstore&,
      ss::sharded<features::feature_table>& feature_table);
    ~disk_log_impl() override;
    disk_log_impl(disk_log_impl&&) noexcept = default;
    disk_log_impl& operator=(disk_log_impl&&) noexcept = delete;
    disk_log_impl(const disk_log_impl&) = delete;
    disk_log_impl& operator=(const disk_log_impl&) = delete;

    ss::future<std::optional<ss::sstring>> close() final;
    ss::future<> remove() final;
    ss::future<> flush() final;
    ss::future<> truncate(truncate_config) final;
    ss::future<> truncate_prefix(truncate_prefix_config) final;
    ss::future<> compact(compaction_config) final;
    ss::future<> do_housekeeping() final override;

    ss::future<model::offset> monitor_eviction(ss::abort_source&) final;

    ss::future<model::record_batch_reader> make_reader(log_reader_config) final;
    ss::future<model::record_batch_reader> make_reader(timequery_config);
    // External synchronization: only one append can be performed at a time.
    log_appender make_appender(log_append_config cfg) final;
    /// timequery
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final;
    size_t segment_count() const final { return _segs.size(); }
    offset_stats offsets() const final;
    model::timestamp start_timestamp() const final;
    std::optional<model::term_id> get_term(model::offset) const final;
    std::optional<model::offset>
    get_term_last_offset(model::term_id term) const final;
    std::ostream& print(std::ostream&) const final;

    ss::future<> maybe_roll(
      model::term_id, model::offset next_offset, ss::io_priority_class);

    // roll immediately with the current term. users should prefer the
    // maybe_call interface which enforces sizing policies.
    ss::future<> force_roll(ss::io_priority_class);

    probe& get_probe() { return _probe; }
    model::term_id term() const;
    segment_set& segments() { return _segs; }
    const segment_set& segments() const { return _segs; }
    size_t bytes_left_before_roll() const;

    size_t size_bytes() const override { return _probe.partition_size(); }
    size_t size_bytes_until_offset(model::offset o) const override;
    ss::future<> update_configuration(ntp_config::default_overrides) final;

    int64_t compaction_backlog() const final;

private:
    friend class disk_log_appender; // for multi-term appends
    friend class disk_log_builder;  // for tests
    friend std::ostream& operator<<(std::ostream& o, const disk_log_impl& d);

    ss::future<model::record_batch_reader>
      make_unchecked_reader(log_reader_config);

    ss::future<model::record_batch_reader>
      make_cached_reader(log_reader_config);

    model::offset read_start_offset() const;

    // Postcondition: _start_offset is at least o and stays >= o in the future.
    // Returns if the update actually took place.
    ss::future<bool> update_start_offset(model::offset o);

    ss::future<> do_compact(
      compaction_config, std::optional<model::offset> = std::nullopt);
    ss::future<compaction_result> compact_adjacent_segments(
      std::pair<segment_set::iterator, segment_set::iterator>,
      storage::compaction_config cfg);
    std::optional<std::pair<segment_set::iterator, segment_set::iterator>>
    find_compaction_range(const compaction_config&);
    ss::future<std::optional<model::offset>> gc(compaction_config);

    ss::future<> remove_empty_segments();

    ss::future<> remove_segment_permanently(
      ss::lw_shared_ptr<segment> segment_to_tombsone,
      std::string_view logging_context_msg);

    ss::future<> new_segment(
      model::offset starting_offset,
      model::term_id term_for_this_segment,
      ss::io_priority_class prio);

    ss::future<> do_truncate(
      truncate_config,
      std::optional<std::pair<ssx::semaphore_units, ssx::semaphore_units>>
        lock_guards);
    ss::future<> remove_full_segments(model::offset o);

    ss::future<> do_truncate_prefix(truncate_prefix_config);
    ss::future<> remove_prefix_full_segments(truncate_prefix_config);

    // Propagate a request to the Raft layer to evict segments up until the
    // specified offest.
    //
    // Returns the new start offset of the log.
    ss::future<model::offset> request_eviction_until_offset(model::offset);

    // These methods search the log for the offset to evict at such that
    // the retention policy is satisfied. If no such offset is found
    // std::nullopt is returned.
    std::optional<model::offset> size_based_gc_max_offset(compaction_config);
    std::optional<model::offset> time_based_gc_max_offset(compaction_config);

    /// Conditionally adjust retention timestamp on any segment that appears
    /// to have invalid timestamps, to ensure retention can proceed.
    ss::future<>
    retention_adjust_timestamps(std::chrono::seconds ignore_in_future);

    compaction_config apply_overrides(compaction_config) const;

    storage_resources& resources();

    void wrote_stm_bytes(size_t);

    compaction_config override_retention_config(compaction_config cfg) const;

    bool is_cloud_retention_active() const;

    std::optional<model::offset> retention_offset(compaction_config);

    ss::future<usage_report> disk_usage(compaction_config);

private:
    size_t max_segment_size() const;
    // Computes the segment size based on the latest max_segment_size
    // configuration. This takes into consideration any segment size
    // overrides since the last time it was called.
    size_t compute_max_segment_size();
    struct eviction_monitor {
        ss::promise<model::offset> promise;
        ss::abort_source::subscription subscription;
    };
    bool _closed{false};
    ss::gate _compaction_housekeeping_gate;
    log_manager& _manager;
    float _segment_size_jitter;
    segment_set _segs;
    kvstore& _kvstore;
    ss::sharded<features::feature_table>& _feature_table;
    model::offset _start_offset;
    // Used to serialize updates to _start_offset. See the update_start_offset
    // method.
    mutex _start_offset_lock;
    lock_manager _lock_mngr;
    storage::probe _probe;
    failure_probes _failure_probes;
    std::optional<eviction_monitor> _eviction_monitor;
    size_t _max_segment_size;
    std::unique_ptr<readers_cache> _readers_cache;
    // average ratio of segment sizes after segment size before compaction
    moving_average<double, 5> _compaction_ratio{1.0};

    // Mutually exclude operations that do non-appending modification
    // to segments: adjacent segment compaction and truncation.  Truncation
    // repeatedly takes+releases segment read locks, and without this extra
    // coarse grained lock, the compaction can happen in between steps.
    // See https://github.com/redpanda-data/redpanda/issues/7118
    mutex _segment_rewrite_lock;

    // Bytes written since last time we requested stm snapshot
    ssx::semaphore_units _stm_dirty_bytes_units;

    // Mutually exclude operations that will cause segment rolling
    // do_housekeeping and maybe_roll
    mutex _segments_rolling_lock;
};

} // namespace storage
