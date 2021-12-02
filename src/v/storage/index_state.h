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

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/fragmented_vector.h"

#include <cstdint>
#include <optional>

namespace storage {
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
 */
struct index_state {
    static constexpr int8_t ondisk_version = 3;

    index_state() = default;
    index_state(index_state&&) noexcept = default;
    index_state& operator=(index_state&&) noexcept = default;
    index_state(const index_state&) = delete;
    index_state& operator=(const index_state&) = delete;
    ~index_state() noexcept = default;

    /// \brief sizeof the index in bytes
    uint32_t size{0};
    /// \brief currently xxhash64
    uint64_t checksum{0};
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

    bool empty() const { return relative_offset_index.empty(); }

    void
    add_entry(uint32_t relative_offset, uint32_t relative_time, uint64_t pos) {
        relative_offset_index.push_back(relative_offset);
        relative_time_index.push_back(relative_time);
        position_index.push_back(pos);
    }
    void pop_back() {
        relative_offset_index.pop_back();
        relative_time_index.pop_back();
        position_index.pop_back();
    }
    std::tuple<uint32_t, uint32_t, uint64_t> get_entry(size_t i) {
        return {
          relative_offset_index[i], relative_time_index[i], position_index[i]};
    }
    iobuf checksum_and_serialize();

    bool maybe_index(
      size_t accumulator,
      size_t step,
      size_t starting_position_in_file,
      model::offset base_offset,
      model::offset batch_max_offset,
      model::timestamp first_timestamp,
      model::timestamp last_timestamp);

    friend bool operator==(const index_state&, const index_state&) = default;

    static std::optional<index_state> hydrate_from_buffer(iobuf);
    friend std::ostream& operator<<(std::ostream&, const index_state&);
};

} // namespace storage
