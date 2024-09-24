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
#include "storage/file_sanitizer_types.h"
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

// this type is meant to force the use of ss::lower_system_clock, since it's
// cheaper than std::system_clock while being good enough for the purpose of
// retention
using broker_timestamp_t = ss::lowres_system_clock::time_point;

// clang-format off
// this truth table shows which timestamps gets used as retention_timestamp
//
// use_broker_ts  has_broker_ts  ignore_future_ts  has_alternative_to_future_ts  retention_ts
//                               (likely false)
// TRUE           TRUE           TRUE              TRUE                          broker_timestamp
// TRUE           TRUE           TRUE              FALSE                         broker_timestamp
// TRUE           TRUE           FALSE             TRUE                          broker_timestamp
// TRUE           TRUE           FALSE             FALSE                         broker_timestamp             // new segment, new cluster
// TRUE           FALSE          TRUE              TRUE                          segment_index::_retention_ms // buggy old segment, new cluster
// TRUE           FALSE          TRUE              FALSE                         max_timestamp
// TRUE           FALSE          FALSE             TRUE                          max_timestamp
// TRUE           FALSE          FALSE             FALSE                         max_timestamp                // old segment, new cluster
// FALSE          TRUE           TRUE              TRUE                          segment_index::_retention_ms
// FALSE          TRUE           TRUE              FALSE                         max_timestamp
// FALSE          TRUE           FALSE             TRUE                          max_timestamp
// FALSE          TRUE           FALSE             FALSE                         max_timestamp                // new segment, upgraded cluster
// FALSE          FALSE          TRUE              TRUE                          segment_index::_retention_ms // buggy old segments
// FALSE          FALSE          TRUE              FALSE                         max_timestamp
// FALSE          FALSE          FALSE             TRUE                          max_timestamp
// FALSE          FALSE          FALSE             FALSE                         max_timestamp                // old segment, upgraded cluster
// clang-format on

// this struct is meant to be a local copy of the feature
// broker_time_based_retention and configuration property
// storage_ignore_timestamps_in_future_secs
struct time_based_retention_cfg {
    bool use_broker_time;
    bool use_escape_hatch_for_timestamps_in_the_future;

    static auto
    make(const features::feature_table& ft) -> time_based_retention_cfg {
        return {
          .use_broker_time = ft.is_active(
            features::feature::broker_time_based_retention),
          .use_escape_hatch_for_timestamps_in_the_future
          = config::shard_local_cfg()
              .storage_ignore_timestamps_in_future_sec()
              .has_value(),
        };
    }

    /// this function is the codification of the table above
    constexpr auto compute_retention_ms(
      std::optional<model::timestamp> broker_ts,
      model::timestamp max_ts,
      std::optional<model::timestamp> alternative_retention_ts) const noexcept {
        // new clusters and new segments should hit this branch
        if (likely(use_broker_time && broker_ts.has_value())) {
            return *broker_ts;
        }
        // don't use broker time or no broker time available. fallback
        if (unlikely(
              use_escape_hatch_for_timestamps_in_the_future
              && alternative_retention_ts.has_value())) {
            return *alternative_retention_ts;
        }
        // If storage_ignore_timestamps_in_future_sec is disabled, then
        // we should not respect _retention_timestamp even if it has
        // been set (this corresponds to the property being toggled on
        // then off again at runtime).
        return max_ts;
    }
};

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
      std::optional<ntp_sanitizer_config> sanitizer_config,
      std::optional<model::timestamp> broker_timestamp = std::nullopt,
      std::optional<model::timestamp> clean_compact_timestamp = std::nullopt,
      bool may_have_tombstone_records = true);

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

    void maybe_track(
      const model::record_batch_header&,
      std::optional<broker_timestamp_t> new_broker_ts,
      size_t filepos);
    std::optional<entry> find_nearest(model::offset);
    std::optional<entry> find_nearest(model::timestamp);
    /// Find entry by file offset (the value may overshoot or find precise
    /// match)
    std::optional<entry> find_above_size_bytes(size_t distance);
    /// Find entry by file offset (the value will undershoot or find precise
    /// match)
    std::optional<entry> find_below_size_bytes(size_t distance);

    /// Fallback timestamp search for if the recorded max ts appears to be
    /// invalid, e.g. too far in the future
    std::optional<model::timestamp>
      find_highest_timestamp_before(model::timestamp) const;

    auto num_compactible_records_appended() const {
        return _state.num_compactible_records_appended;
    }
    model::offset base_offset() const { return _state.base_offset; }
    model::offset max_offset() const { return _state.max_offset; }
    model::timestamp max_timestamp() const { return _state.max_timestamp; }
    model::timestamp base_timestamp() const { return _state.base_timestamp; }
    // this is the broker timestamp of the last record in the index, used by
    // time-based retention. introduced in v23.3, indices created before this
    // version do not have it and will rely on max_timestamp
    std::optional<model::timestamp> broker_timestamp() const {
        return _state.broker_timestamp;
    }

    bool batch_timestamps_are_monotonic() const {
        return _state.batch_timestamps_are_monotonic;
    }
    bool non_data_timestamps() const { return _state.non_data_timestamps; }

    /// this method is used in conjuction with
    /// storage_ignore_future_timestamps_secs. For new indices, where
    /// broker_timestamps is set, this value is not used
    void set_retention_timestamp(model::timestamp t) {
        _retention_timestamp = t;
    }

    model::timestamp retention_timestamp(time_based_retention_cfg cfg) const {
        return cfg.compute_retention_ms(
          _state.broker_timestamp, _state.max_timestamp, _retention_timestamp);
    }

    // Check if compacted timestamp has a value.
    bool has_clean_compact_timestamp() const {
        return _state.clean_compact_timestamp.has_value();
    }

    // Set the compacted timestamp, if it doesn't already have a value.
    void maybe_set_clean_compact_timestamp(model::timestamp t) {
        if (!_state.clean_compact_timestamp.has_value()) {
            _state.clean_compact_timestamp = t;
            _needs_persistence = true;
        }
    }

    // Get the compacted timestamp.
    std::optional<model::timestamp> clean_compact_timestamp() const {
        return _state.clean_compact_timestamp;
    }

    void set_may_have_tombstone_records(bool b) {
        if (_state.may_have_tombstone_records != b) {
            _needs_persistence = true;
        }
        _state.may_have_tombstone_records = b;
    }

    bool may_have_tombstone_records() const {
        return _state.may_have_tombstone_records;
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
    std::optional<ntp_sanitizer_config> _sanitizer_config;

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
    friend class segment_index_observer;

    friend std::ostream& operator<<(std::ostream&, const segment_index&);
};

using segment_index_ptr = std::unique_ptr<segment_index>;
std::ostream& operator<<(std::ostream&, const segment_index_ptr&);
std::ostream&
operator<<(std::ostream&, const std::optional<segment_index::entry>&);

} // namespace storage

template<>
struct fmt::formatter<storage::time_based_retention_cfg>
  : public fmt::formatter<std::string_view> {
    auto format(const storage::time_based_retention_cfg& cfg, auto& ctx) const {
        auto str = ssx::sformat(
          "[.use_broker_time={}, "
          ".use_escape_hatch_for_timestamps_in_the_future={}]",
          cfg.use_broker_time,
          cfg.use_escape_hatch_for_timestamps_in_the_future);
        return formatter<std::string_view>::format(
          {str.data(), str.size()}, ctx);
    }
};
