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
#include "storage/offset_translator.h"
#include "storage/probe.h"
#include "storage/readers_cache.h"
#include "storage/types.h"
#include "utils/moving_average.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>

#include <absl/container/flat_hash_map.h>

namespace storage {

/// \brief offset boundary type
///
/// indicate whether or not the offset that encodes the end of the offset
/// range belongs to the offset range
enum class boundary_type {
    exclusive,
    inclusive,
};

class disk_log_impl final : public log {
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
      raft::group_id,
      log_manager&,
      segment_set,
      kvstore&,
      ss::sharded<features::feature_table>& feature_table,
      std::vector<model::record_batch_type> translator_batch_types);
    ~disk_log_impl() override;
    disk_log_impl(disk_log_impl&&) noexcept = delete;
    disk_log_impl& operator=(disk_log_impl&&) noexcept = delete;
    disk_log_impl(const disk_log_impl&) = delete;
    disk_log_impl& operator=(const disk_log_impl&) = delete;

    ss::future<> start(std::optional<truncate_prefix_config>) final;
    ss::future<std::optional<ss::sstring>> close() final;
    ss::future<> remove() final;
    ss::future<> flush() final;
    ss::future<> truncate(truncate_config) final;
    ss::future<> truncate_prefix(truncate_prefix_config) final;
    ss::future<> housekeeping(housekeeping_config) final;
    ss::future<> apply_segment_ms() final;
    ss::future<> gc(gc_config) final;

    ss::future<model::offset> monitor_eviction(ss::abort_source&) final;

    /// Compute number of bytes between the two offset (including both offsets)
    ///
    /// The 'first' offset should be the first offset of the batch. The 'last'
    /// should be the last offset of the batch. The offset range is inclusive.
    ss::future<std::optional<offset_range_size_result_t>> offset_range_size(
      model::offset first,
      model::offset last,
      ss::io_priority_class io_priority) override;

    /// Find the offset range based on size requirements
    ///
    /// The 'first' offset should be the first offset of the batch. The 'target'
    /// contains size requirements. The desired target size and smallest
    /// acceptable size.
    ss::future<std::optional<offset_range_size_result_t>> offset_range_size(
      model::offset first,
      offset_range_size_requirements_t target,
      ss::io_priority_class io_priority) override;

    /// Return true if the offset range contains compacted data
    bool is_compacted(model::offset first, model::offset last) const override;

    ss::future<model::record_batch_reader> make_reader(log_reader_config) final;
    ss::future<model::record_batch_reader> make_reader(timequery_config);
    // External synchronization: only one append can be performed at a time.
    log_appender make_appender(log_append_config cfg) final;
    /// timequery
    ss::future<std::optional<timequery_result>>
    timequery(timequery_config cfg) final;
    size_t segment_count() const final { return _segs.size(); }
    bool is_new_log() const final;
    offset_stats offsets() const final;
    ss::lw_shared_ptr<const storage::offset_translator_state>
    get_offset_translator_state() const final {
        return _offset_translator.state();
    }
    storage::offset_translator& offset_translator() {
        return _offset_translator;
    }
    model::offset_delta offset_delta(model::offset) const final;
    model::offset from_log_offset(model::offset) const final;
    model::offset to_log_offset(model::offset) const final;
    model::offset find_last_term_start_offset() const final;
    model::timestamp start_timestamp() const final;
    std::optional<model::term_id> get_term(model::offset) const final;
    std::optional<model::offset>
    get_term_last_offset(model::term_id term) const final;
    std::optional<model::offset> index_lower_bound(model::offset o) const final;
    std::ostream& print(std::ostream&) const final;

    // Must be called while _segments_rolling_lock is held.
    ss::future<> maybe_roll_unlocked(
      model::term_id, model::offset next_offset, ss::io_priority_class);

    // Kicks off a background flush of offset translator state to the kvstore.
    void bg_checkpoint_offset_translator();

    ss::future<> force_roll(ss::io_priority_class) override;

    probe& get_probe() override { return *_probe; }
    model::term_id term() const;
    segment_set& segments() override { return _segs; }
    const segment_set& segments() const override { return _segs; }
    size_t bytes_left_before_roll() const;

    size_t size_bytes() const override { return _probe->partition_size(); }
    uint64_t size_bytes_after_offset(model::offset o) const override;
    void set_overrides(ntp_config::default_overrides) final;
    bool notify_compaction_update() final;

    int64_t compaction_backlog() const final;

    ss::future<usage_report> disk_usage(gc_config) override;

    /*
     * Interface for disk space management (see resource_mgmt/storage.cc).
     *
     * Manager will use `cloud_gc_eligible_segments` to determine the point at
     * which the partition should be prefix truncated, subject to data being
     * uploaded into the cloud.
     *
     * Finally, the target offset is set via `set_cloud_gc_offset` which will be
     * used to configure garbage collection during the next GC pass at which
     * point the request will be cleared.
     *
     * Caller will generally configure many partitions and then trigger GC
     * across all partitions.
     */
    auto& gate() { return _compaction_housekeeping_gate; }
    fragmented_vector<ss::lw_shared_ptr<segment>> cloud_gc_eligible_segments();
    void set_cloud_gc_offset(model::offset) override;

    ss::future<reclaimable_offsets>
    get_reclaimable_offsets(gc_config cfg) override;

    std::optional<ssx::semaphore_units> try_segment_roll_lock() {
        return _segments_rolling_lock.try_get_units();
    }

    ss::future<ssx::semaphore_units> segment_roll_lock() {
        return _segments_rolling_lock.get_units();
    }

    size_t reclaimable_size_bytes() const override;

    std::optional<model::offset> retention_offset(gc_config) const final;

    // Collects an iterable list of segments over which to perform sliding
    // window compaction. This can include segments which have already had their
    // keys de-duplicated in every segment between the start of the log and
    // themselves (these are referred to as "clean" segments). These segments
    // would be no-ops to include in sliding window compaction, but they are
    // included in the range anyways in order to allow for timely tombstone
    // record removal via self-compaction, and to ensure that this function
    // returns a contiguous range of segments. It is up to the caller to filter
    // out these already cleanly-compacted segments.
    segment_set find_sliding_range(
      const compaction_config& cfg,
      std::optional<model::offset> new_start_offset = std::nullopt);

    void
    set_last_compaction_window_start_offset(std::optional<model::offset> o) {
        _last_compaction_window_start_offset = o;
    }

    const std::optional<model::offset>&
    get_last_compaction_window_start_offset() const {
        return _last_compaction_window_start_offset;
    }

    readers_cache& readers() { return *_readers_cache; }

    storage_resources& resources();

    // Performs self-compaction on the earliest segment possible, and then
    // attempts to perform compaction on adjacent segments.
    ss::future<> adjacent_merge_compact(
      compaction_config,
      std::optional<model::offset> new_start_offset = std::nullopt);

    ss::future<bool> sliding_window_compact(
      const compaction_config& cfg,
      std::optional<model::offset> new_start_offset = std::nullopt);

    const auto& compaction_ratio() const { return _compaction_ratio; }

    static ss::future<> copy_kvstore_state(
      model::ntp,
      storage::kvstore& source_kvs,
      ss::shard_id target_shard,
      ss::sharded<storage::api>&);

    static ss::future<>
    remove_kvstore_state(const model::ntp&, storage::kvstore&);

    size_t max_segment_size() const;

private:
    friend class disk_log_appender; // for multi-term appends
    friend class disk_log_builder;  // for tests
    friend std::ostream& operator<<(std::ostream& o, const disk_log_impl& d);

    /// Compute file offset of the batch inside the segment
    ss::future<size_t> get_file_offset(
      ss::lw_shared_ptr<segment> s,
      std::optional<segment_index::entry> index_entry,
      model::offset target,
      boundary_type boundary,
      ss::io_priority_class priority);

    ss::future<model::record_batch_reader>
      make_unchecked_reader(log_reader_config);

    ss::future<model::record_batch_reader>
      make_cached_reader(log_reader_config);

    model::offset read_start_offset() const;

    // Postcondition: _start_offset is at least o and stays >= o in the future.
    // Returns if the update actually took place.
    ss::future<bool> update_start_offset(model::offset o);

    // Finds a range of adjacent segments that can be compacted together.
    // A valid segment range consists of segments with the same raft term, and a
    // combined size less than max_compacted_segment_size.
    //
    // Returns std::nullopt if a valid range of two segments could not be found.
    // Otherwise, a pair of iterators to the segments.
    std::optional<std::pair<segment_set::iterator, segment_set::iterator>>
    find_adjacent_compaction_range(const compaction_config& cfg);

    // Requests compaction of adjacent segments per the max_collectible_offset
    // in the compaction_config.
    //
    // Returns std::nullopt if an adjacent pair could not be found for adjacent
    // compaction, or a compaction_result. This result should also have its
    // did_compact() function checked to verify whether adjacent segment
    // compaction actually occurred.
    ss::future<std::optional<compaction_result>>
    compact_adjacent_segments(storage::compaction_config cfg);

    // Performs compaction of adjacent segments. This pair of segments is
    // physically combined to replace the first segment. The resulting combined
    // segment is then self-compacted, and the redundant second segment is
    // permanently removed.
    // Currently, only two adjacent segments can be compacted at a time (i.e,
    // there must be, and are only, two segments in the range).
    //
    // Returns a compaction_result indicating whether or not compaction was
    // executed, and the total size of the segments before and after the
    // operation.
    ss::future<compaction_result> do_compact_adjacent_segments(
      std::pair<segment_set::iterator, segment_set::iterator>,
      storage::compaction_config cfg);

    ss::future<std::optional<model::offset>> do_gc(gc_config);
    ss::future<> do_compact(
      compaction_config, std::optional<model::offset> new_start_offset);

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
    std::optional<model::offset> size_based_gc_max_offset(gc_config) const;
    std::optional<model::offset> time_based_gc_max_offset(gc_config) const;

    /// Conditionally adjust retention timestamp on any segment that appears
    /// to have invalid timestamps, to ensure retention can proceed.
    ss::future<> maybe_adjust_retention_timestamps();

    gc_config apply_overrides(gc_config) const;
    gc_config apply_kafka_retention_overrides(gc_config) const;

    void wrote_stm_bytes(size_t);

    // returns true if this partition's local retention configuration has
    // overrides, such as custom topic configs.
    bool has_local_retention_override() const;

    gc_config maybe_apply_local_storage_overrides(gc_config) const;
    gc_config apply_local_storage_overrides(gc_config) const;

    bool is_cloud_retention_active() const;

    // returns retention_offset(cfg) but may also first apply adjustments to
    // future timestamps if this option is turned on in configuration.
    ss::future<std::optional<model::offset>>
    maybe_adjusted_retention_offset(gc_config cfg);

    /*
     * total disk usage and the amount of reclaimable space are most efficiently
     * computed together given that use cases often use both together.
     */
    ss::future<std::pair<usage, reclaim_size_limits>>
      disk_usage_and_reclaimable_space(gc_config);

    ss::future<usage_target> disk_usage_target(gc_config, usage);
    ss::future<std::optional<size_t>>
      disk_usage_target_time_retention(gc_config);

    size_t get_log_truncation_counter() const noexcept override;

private:
    // Computes the segment size based on the latest max_segment_size
    // configuration. This takes into consideration any segment size
    // overrides since the last time it was called.
    size_t compute_max_segment_size();
    struct eviction_monitor {
        ss::promise<model::offset> promise;
        ss::abort_source::subscription subscription;
    };
    bool _closed{false};
    ss::abort_source _compaction_as;
    ss::gate _compaction_housekeeping_gate;
    log_manager& _manager;
    float _segment_size_jitter;
    segment_set _segs;
    kvstore& _kvstore;
    ss::sharded<features::feature_table>& _feature_table;
    model::offset _start_offset;
    // Used to serialize updates to _start_offset. See the update_start_offset
    // method.
    mutex _start_offset_lock{"disk_log_impl::start_offset_lock"};
    lock_manager _lock_mngr;
    storage::offset_translator _offset_translator;

    std::unique_ptr<storage::probe> _probe;
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
    mutex _segment_rewrite_lock{"segment_rewrite_lock"};

    // Bytes written since last time we requested stm snapshot
    ssx::semaphore_units _stm_dirty_bytes_units;

    // Mutually exclude operations that will cause segment rolling
    // do_housekeeping and maybe_roll
    //
    // This lock will only rarely be contended. If it is held, then we must
    // wait for housekeeping or truncation to complete before proceeding,
    // because the log might be in a state mid-roll where it has no appender.
    // We need to take this irrespective of whether we're actually rolling or
    // not, in order to ensure that writers wait for a background roll to
    // complete if one is ongoing.
    mutex _segments_rolling_lock{"segments_rolling_lock"};
    // This counter is incremented when the log is truncated. It doesn't
    // count logical truncations and can be incremented multiple times.
    size_t _suffix_truncation_indicator{0};

    std::optional<model::offset> _cloud_gc_offset;

    // The offset at which the last window compaction finished, above which keys
    // have been fully deduplicated. The next round of window compaction
    // can skip segments above this offset, if no new segments have been created
    // since last window compaction.
    std::optional<model::offset> _last_compaction_window_start_offset;

    size_t _reclaimable_size_bytes{0};
    bool _compaction_enabled;
};

} // namespace storage
