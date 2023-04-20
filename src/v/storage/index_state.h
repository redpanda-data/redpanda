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

#include "bytes/iobuf.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/sharded.hh>

#include <cstdint>
#include <optional>

namespace storage {

using offset_delta_time = ss::bool_class<struct offset_delta_time_tag>;

/*
 * In order to be able to represent negative time deltas (required for
 * out of order timestamps, the time delta stored in 'index_state' is
 * offset by 2^31. The 'offset_time_index' class below deals with
 * this translation. The range for time deltas is roughly from -596h to +596h.
 */
class offset_time_index {
public:
    static constexpr model::timestamp::type offset = 2147483648; // 2^31
    static constexpr model::timestamp::type delta_time_min = -offset;
    static constexpr model::timestamp::type delta_time_max = offset - 1;

    offset_time_index(model::timestamp ts, offset_delta_time with_offset)
      : _with_offset(with_offset) {
        if (_with_offset == offset_delta_time::yes) {
            _val = static_cast<uint32_t>(
              std::clamp(ts(), delta_time_min, delta_time_max) + offset);
        } else {
            _val = _val = static_cast<uint32_t>(std::clamp(
              ts(),
              model::timestamp::type{std::numeric_limits<uint32_t>::min()},
              model::timestamp::type{std::numeric_limits<uint32_t>::max()}));
        }
    }

    uint32_t operator()() const {
        if (_with_offset == offset_delta_time::yes) {
            return _val - static_cast<uint32_t>(offset);
        } else {
            return _val;
        }
    }

private:
    offset_time_index(uint32_t val, offset_delta_time with_offset)
      : _with_offset(with_offset)
      , _val(val) {}

    uint32_t raw_value() const { return _val; }

    offset_delta_time _with_offset;
    uint32_t _val;

    friend struct index_state;
};

/* Fileformat:
   1 byte  - version
   4 bytes - size - does not include the version or size
   8 bytes - checksum - xxhash32 -- we checksum everything below the checksum
   4 bytes - bitflags - unused
   8 bytes - based_offset
   8 bytes - max_offset
   8 bytes - base_time
   8 bytes - max_time
   4 bytes - index.size()
   [] relative_offset_index
   [] relative_time_index
   [] position_index
   1 byte  - batch_timestamps_are_monotonic
   1 byte  - with_offset
   1 byte  - non_data_timestamps
 */
struct index_state
  : serde::envelope<index_state, serde::version<5>, serde::compat_version<4>> {
    static constexpr auto monotonic_timestamps_version = 5;

    static index_state make_empty_index(offset_delta_time with_offset);

    index_state() = default;

    index_state(index_state&&) noexcept = default;
    index_state& operator=(index_state&&) noexcept = default;
    index_state& operator=(const index_state&) = delete;
    ~index_state() noexcept = default;

    index_state copy() const { return *this; }

    /// \brief unused
    uint32_t bitflags{0};
    // the batch's base_offset of the first batch
    model::offset base_offset{0};
    // it is the batch's last_offset of the last batch
    model::offset max_offset{0};
    // the batch's base_timestamp of the first batch
    model::timestamp base_timestamp{0};
    // the batch's max_timestamp of the last batch
    model::timestamp max_timestamp{0};

    /// breaking indexes into their own has a 6x latency reduction
    fragmented_vector<uint32_t> relative_offset_index;
    fragmented_vector<uint32_t> relative_time_index;
    fragmented_vector<uint64_t> position_index;

    // flag indicating whether the maximum timestamp on the batches
    // of this segment are monontonically increasing.
    bool batch_timestamps_are_monotonic{true};

    // flag indicating whether the relative time index has been offset
    offset_delta_time with_offset{false};

    // flag indicating whether this segment contains non user-data timestamps
    bool non_data_timestamps{false};

    size_t size() const { return relative_offset_index.size(); }

    bool empty() const { return relative_offset_index.empty(); }

    void add_entry(
      uint32_t relative_offset, offset_time_index relative_time, uint64_t pos) {
        relative_offset_index.push_back(relative_offset);
        relative_time_index.push_back(relative_time.raw_value());
        position_index.push_back(pos);
    }
    void pop_back() {
        relative_offset_index.pop_back();
        relative_time_index.pop_back();
        position_index.pop_back();
        if (empty()) {
            non_data_timestamps = false;
        }
    }
    std::tuple<uint32_t, offset_time_index, uint64_t>
    get_entry(size_t i) const {
        return {
          relative_offset_index[i],
          offset_time_index{relative_time_index[i], with_offset},
          position_index[i]};
    }

    void shrink_to_fit() {
        relative_offset_index.shrink_to_fit();
        relative_time_index.shrink_to_fit();
        position_index.shrink_to_fit();
    }

    std::optional<std::tuple<uint32_t, offset_time_index, uint64_t>>
    find_entry(model::timestamp ts) {
        const auto idx = offset_time_index{ts, with_offset};

        auto it = std::lower_bound(
          std::begin(relative_time_index),
          std::end(relative_time_index),
          idx.raw_value(),
          std::less<uint32_t>{});
        if (it == relative_offset_index.end()) {
            return std::nullopt;
        }

        const auto dist = std::distance(relative_offset_index.begin(), it);

        // lower_bound will place us on the first batch in the index that has
        // 'max_timestamp' greater than 'ts'. Since not every batch is indexed,
        // it's not guaranteed* that 'ts' will be present in the batch
        // (i.e. 'ts > first_timestamp'). For this reason, we go back one batch.
        //
        // *In the case where lower_bound places on the first batch, we'll
        // start the timequery from the beggining of the segment as the user
        // data batch is always indexed.
        return get_entry(dist > 0 ? dist - 1 : 0);
    }

    bool maybe_index(
      size_t accumulator,
      size_t step,
      size_t starting_position_in_file,
      model::offset base_offset,
      model::offset batch_max_offset,
      model::timestamp first_timestamp,
      model::timestamp last_timestamp,
      bool user_data);

    void update_batch_timestamps_are_monotonic(bool pred) {
        batch_timestamps_are_monotonic = batch_timestamps_are_monotonic && pred;
    }

    friend bool operator==(const index_state&, const index_state&) = default;

    friend std::ostream& operator<<(std::ostream&, const index_state&);

    void serde_write(iobuf&) const;
    friend void read_nested(iobuf_parser&, index_state&, const size_t);

private:
    index_state(const index_state& o) noexcept
      : bitflags(o.bitflags)
      , base_offset(o.base_offset)
      , max_offset(o.max_offset)
      , base_timestamp(o.base_timestamp)
      , max_timestamp(o.max_timestamp)
      , relative_offset_index(o.relative_offset_index.copy())
      , relative_time_index(o.relative_time_index.copy())
      , position_index(o.position_index.copy())
      , batch_timestamps_are_monotonic(o.batch_timestamps_are_monotonic)
      , with_offset(o.with_offset) {}
};

} // namespace storage
