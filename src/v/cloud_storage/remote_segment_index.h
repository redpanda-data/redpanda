/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/parser.h"
#include "units.h"
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
      kafka::offset initial_kaf,
      int64_t initial_file_pos,
      int64_t file_pos_step);

    /// Add new tuple to the index.
    void
    add(model::offset rp_offset, kafka::offset kaf_offset, int64_t file_offset);

    struct find_result {
        model::offset rp_offset;
        kafka::offset kaf_offset;
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
    std::optional<find_result> find_kaf_offset(kafka::offset upper_bound);

    /// Serialize offset_index
    iobuf to_iobuf();

    /// Deserialize offset_index
    void from_iobuf(iobuf in);

private:
    struct index_value {
        size_t ix;
        int64_t value;
    };

    /// Find index entry which is strictly lower than the provided value
    ///
    /// The encoder and the write buffer have to be provided via parameters.
    /// The returned value is a variant which contains a monostate if no
    /// value can be found; index_value if the value is found in the
    /// encoder; find_result if the value is found in the write buffer (in
    /// this case no further search is needed).
    std::variant<std::monostate, index_value, find_result> maybe_find_offset(
      int64_t upper_bound,
      deltafor_encoder<int64_t>& encoder,
      const std::array<int64_t, buffer_depth>& write_buffer);

    /// Find element inside the offset range stored in the decoder which is
    /// less than offset. Return last element if no such element can be
    /// found. Return nullopt if all emlements are larger or equal than
    /// offset.
    static std::optional<index_value>
    _find_under(deltafor_decoder<int64_t> decoder, int64_t offset);

    /// Return element by index.
    template<class DecoderT>
    static std::optional<int64_t>
    _fetch_ix(DecoderT decoder, size_t target_ix) {
        size_t ix = 0;
        std::array<int64_t, buffer_depth> buffer{};
        while (decoder.read(buffer)) {
            for (auto o : buffer) {
                if (ix == target_ix) {
                    return o;
                }
                ix++;
            }
            buffer = {};
        }
        return std::nullopt;
    }

private:
    std::array<int64_t, buffer_depth> _rp_offsets;
    std::array<int64_t, buffer_depth> _kaf_offsets;
    std::array<int64_t, buffer_depth> _file_offsets;
    uint64_t _pos;
    model::offset _initial_rp;
    kafka::offset _initial_kaf;
    int64_t _initial_file_pos;

    using encoder_t = deltafor_encoder<int64_t>;
    using decoder_t = deltafor_decoder<int64_t>;
    using delta_delta_t = details::delta_delta<int64_t>;
    using foffset_encoder_t = deltafor_encoder<int64_t, delta_delta_t>;
    using foffset_decoder_t = deltafor_decoder<int64_t, delta_delta_t>;

    encoder_t _rp_index;
    encoder_t _kaf_index;
    foffset_encoder_t _file_index;
    int64_t _min_file_pos_step;
};

class remote_segment_index_builder : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    remote_segment_index_builder(
      offset_index& ix,
      model::offset_delta initial_delta,
      size_t sampling_step);

    virtual consume_result
    accept_batch_start(const model::record_batch_header&) const;

    virtual void consume_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk);

    virtual void skip_batch_start(
      model::record_batch_header,
      size_t physical_base_offset,
      size_t size_on_disk);

    virtual void consume_records(iobuf&&);
    virtual stop_parser consume_batch_end();
    virtual void print(std::ostream&) const;

private:
    offset_index& _ix;
    model::offset_delta _running_delta;
    size_t _window{0};
    size_t _sampling_step;
};

inline ss::lw_shared_ptr<storage::continuous_batch_parser>
make_remote_segment_index_builder(
  ss::input_stream<char> stream,
  offset_index& ix,
  model::offset_delta initial_delta,
  size_t sampling_step) {
    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::make_unique<remote_segment_index_builder>(
        ix, initial_delta, sampling_step),
      storage::segment_reader_handle(std::move(stream)));
    return parser;
}

} // namespace cloud_storage
