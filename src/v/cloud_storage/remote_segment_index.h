/*
 * Copyright 2022 Vectorized, Inc.
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
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/parser.h"
#include "utils/delta_for.h"

#include <seastar/util/log.hh>

#include <variant>

namespace cloud_storage {

/// Offset index for remote_segment
///
/// The object indexes tuples that contain three elements:
/// - redpanda offset
/// - kafka offset
/// - file offset
///
/// The search is linear. The underlying data structure is a
/// fragmented buffer (iobuf). It is possible to search by redpanda
/// and kafka offsets, but not by file offset.
///
/// The invariant of the offset_index is that all three encoders
/// have the same number of elements. All three buffers should also
/// have the same number of elements.
class offset_index {
    static constexpr uint32_t buffer_depth = details::FOR_buffer_depth;
    static constexpr uint32_t index_mask = buffer_depth - 1;
    static_assert(
      (buffer_depth & index_mask) == 0,
      "buffer_depth have to be a power of two");

public:
    offset_index(
      model::offset initial_rp,
      model::offset initial_kaf,
      int64_t initial_file_pos);

    /// Add new tuple to the index.
    void
    add(model::offset rp_offset, model::offset kaf_offset, int64_t file_offset);

    struct find_result {
        model::offset rp_offset;
        model::offset kaf_offset;
        int64_t file_pos;
    };

    /// Find index entry which is strictly lower than the redpanda offset
    ///
    /// The returned value has rp_offset less than upper_bound.
    /// If all elements are larger than 'upper_bound' nullopt is returned.
    /// If all elements are smaller than 'upper_bound' the last value is
    /// returned.
    std::optional<find_result> find_rp_offset(model::offset upper_bound);

    /// Find index entry which is strictly lower than the kafka offset
    ///
    /// The returned value has kaf_offset less than upper_bound.
    /// If all elements are larger than 'upper_bound' nullopt is returned.
    /// If all elements are smaller than 'upper_bound' the last value is
    /// returned.
    std::optional<find_result> find_kaf_offset(model::offset upper_bound);

private:
    struct index_value {
        size_t ix;
        int64_t value;
    };

    /// Find index entry which is strictly lower than the provided value
    ///
    /// The encoder and the write buffer have to be provided via parameters.
    /// The returned value is a variant which contains a monostate if no value
    /// can be found; index_value if the value is found in the encoder;
    /// find_result if the value is found in the write buffer (in this case no
    /// further search is needed).
    std::variant<std::monostate, index_value, find_result> maybe_find_offset(
      model::offset upper_bound,
      deltafor_encoder<int64_t>& encoder,
      const std::array<int64_t, buffer_depth>& write_buffer);

    /// Find element inside the offset range stored in the decoder which is
    /// less than offset. Return last element if no such element can be found.
    /// Return nullopt if all emlements are larger or equal than offset.
    static std::optional<index_value>
    _find_under(deltafor_decoder<int64_t> decoder, int64_t offset);

    /// Return element by index.
    static std::optional<int64_t>
    _fetch_ix(deltafor_decoder<int64_t> decoder, size_t target_ix);

private:
    std::array<int64_t, buffer_depth> _rp_offsets;
    std::array<int64_t, buffer_depth> _kaf_offsets;
    std::array<int64_t, buffer_depth> _file_offsets;
    uint64_t _pos;
    model::offset _initial_rp;
    model::offset _initial_kaf;
    int64_t _initial_file_pos;
    deltafor_encoder<int64_t> _rp_encoder;
    deltafor_encoder<int64_t> _kaf_encoder;
    deltafor_encoder<int64_t> _file_encoder;
};

} // namespace cloud_storage
