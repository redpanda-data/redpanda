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
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/fs_utils.h"
#include "storage/index_state.h"
#include "storage/types.h"

#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/unaligned.hh>

#include <memory>
#include <optional>
#include <vector>

namespace storage {

/**
 * file file format is: [ header ] [ payload ]
 * header  == segment_index::header
 * payload == std::vector<pair<uint32_t,uint32_t>>;
 *
 * Assume an ntp("default", "test", 0);
 *     default/test/0/1-1-v1.log
 *
 * The name of this index _must_ be then:
 *     default/test/0/1-1-v1.base_index
 */
class segment_index {
public:
    /// brief hydrated entry
    struct entry {
        model::offset offset;
        model::timestamp timestamp;
        size_t filepos;
        friend std::ostream& operator<<(std::ostream&, const entry&);
    };

    // 32KB - a well known number as a sweet spot for fetching data from disk
    static constexpr size_t default_data_buffer_step = 4096 * 8;

    segment_index(
      segment_full_path path,
      model::offset base,
      size_t step,
      ss::sharded<features::feature_table>& feature_table,
      debug_sanitize_files);

    ~segment_index() noexcept = default;
    segment_index(segment_index&&) noexcept = default;
    segment_index& operator=(segment_index&&) noexcept = default;
    segment_index(const segment_index&) = delete;
    segment_index& operator=(const segment_index&) = delete;

    /**
     * Estimate the size of an index file for a log segment
     * of a certain size.
     */
    static uint64_t estimate_size(uint64_t log_size) {
        // Index entry every `step` bytes, each entry is 16 bytes
        // plus one entry that is with the first batch.
        return 1 + 16 * log_size / default_data_buffer_step;
    }

    void maybe_track(const model::record_batch_header&, size_t filepos);
    std::optional<entry> find_nearest(model::offset);
    std::optional<entry> find_nearest(model::timestamp);

    /// Fallback timestamp search for if the recorded max ts appears to be
    /// invalid, e.g. too far in the future
    std::optional<model::timestamp>
      find_highest_timestamp_before(model::timestamp) const;

    model::offset base_offset() const { return _state.base_offset; }
    model::offset max_offset() const { return _state.max_offset; }
    model::timestamp max_timestamp() const { return _state.max_timestamp; }
    model::timestamp base_timestamp() const { return _state.base_timestamp; }
    bool batch_timestamps_are_monotonic() const {
        return _state.batch_timestamps_are_monotonic;
    }
    bool non_data_timestamps() const { return _state.non_data_timestamps; }

    void set_retention_timestamp(model::timestamp t) {
        _retention_timestamp = t;
    }
    model::timestamp retention_timestamp() const {
        if (unlikely(config::shard_local_cfg()
                       .storage_ignore_timestamps_in_future_sec())) {
            return _retention_timestamp.value_or(_state.max_timestamp);
        } else {
            // If storage_ignore_timestamps_in_future_sec is disabled, then
            // we should not respect _retention_timestamp even if it has
            // been set (this corresponds to the property being toggled on
            // then off again at runtime).
            return _state.max_timestamp;
        }
    }

    ss::future<bool> materialize_index();
    ss::future<> flush();
    ss::future<> truncate(model::offset, model::timestamp);

    ss::future<ss::file> open();

    const segment_full_path& path() const { return _path; }
    size_t size() const { return _state.size(); }

    /// \brief erases the underlying file and resets the index
    /// this is used during compacted index recovery, as we must first
    /// invalidate all indices, before we swap the data file
    ss::future<> drop_all_data();

    /// \brief resets the state to 0, except for base_offset
    /// a destructive operation. Needed for node bootstrap
    void reset();
    void swap_index_state(index_state&&);
    bool needs_persistence() const { return _needs_persistence; }
    index_state release_index_state() && { return std::move(_state); }

    /*
     * Get the on-disk device usage size of the index.
     */
    ss::future<size_t> disk_usage();
    void clear_cached_disk_usage() { _disk_usage_size.reset(); }

private:
    ss::future<bool> materialize_index_from_file(ss::file);
    ss::future<> flush_to_file(ss::file);

    segment_full_path _path;
    size_t _step;
    std::reference_wrapper<ss::sharded<features::feature_table>> _feature_table;
    size_t _acc{0};
    bool _needs_persistence{false};
    index_state _state;
    debug_sanitize_files _sanitize;

    // Override the timestamp used for retention, in case what's in
    // the index _state is no good.
    std::optional<model::timestamp> _retention_timestamp;

    // invalidate size cache when on-disk size may change
    std::optional<size_t> _disk_usage_size;

    model::timestamp _last_batch_max_timestamp;

    /** Constructor with mock file content for unit testing */
    segment_index(
      segment_full_path path,
      ss::file mock_file,
      model::offset base,
      size_t step,
      ss::sharded<features::feature_table>& feature_table);

    // For unit testing only.  If this is set, then open() returns
    // the contents of mock_file instead of opening the path in _name.
    std::optional<ss::file> _mock_file;

    friend class offset_index_utils_fixture;
    friend class log_replayer_fixture;

    friend std::ostream& operator<<(std::ostream&, const segment_index&);
};

using segment_index_ptr = std::unique_ptr<segment_index>;
std::ostream& operator<<(std::ostream&, const segment_index_ptr&);
std::ostream&
operator<<(std::ostream&, const std::optional<segment_index::entry>&);

} // namespace storage
