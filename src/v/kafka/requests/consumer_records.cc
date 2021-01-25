// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/consumer_records.h"

#include "kafka/requests/kafka_batch_adapter.h"
#include "model/record.h"

namespace kafka {

namespace {

/// \brief A subset of the model::record_batch_header
///
/// Just enough to delineate record_batch within consumer_records.
struct record_batch_info {
    model::offset base_offset{};
    int32_t record_count{};
    int32_t size_bytes{};

    int32_t record_bytes() {
        return size_bytes - model::packed_record_batch_header_size;
    }
    model::offset last_offset() {
        return base_offset + model::offset{record_count - 1};
    }
};

record_batch_info read_record_batch_info(iobuf_const_parser& in) {
    const size_t initial_bytes_consumed = in.bytes_consumed();
    const auto base_offset = model::offset(in.consume_be_type<int64_t>());
    const auto batch_length = in.consume_be_type<int32_t>();
    constexpr size_t skip_len_1 = sizeof(int32_t) + // partition_leader_epoch
                                  sizeof(int8_t) +  // magic
                                  sizeof(int32_t) + // crc
                                  sizeof(int16_t);  // attrs

    in.skip(skip_len_1);
    const auto last_offset_delta = in.consume_be_type<int32_t>();
    constexpr size_t skip_len_2 = sizeof(int64_t) + // first_timestamp
                                  sizeof(int64_t) + // max_timestamp
                                  sizeof(int64_t) + // producer_id
                                  sizeof(int16_t) + // producer_epoch
                                  sizeof(int32_t);  // base_sequence
    in.skip(skip_len_2);
    const auto record_count = in.consume_be_type<int32_t>();

    // size_bytes are  the normal kafka batch length minus the `IGNORED`
    const int32_t size_bytes
      = batch_length - internal::kafka_header_size
        + model::packed_record_batch_header_size
        // Kafka *does not* include the first 2 fields in the size calculation
        // they build the types bottoms up, not top down
        + sizeof(base_offset) + sizeof(batch_length);

    if (unlikely(record_count - 1 != last_offset_delta)) {
        throw std::runtime_error(fmt::format(
          "Invalid kafka header parsing. "
          "record count:{}, but"
          "base_offset: {} and last_offset_delta: {}",
          record_count,
          base_offset,
          last_offset_delta));
    }

    const size_t total_bytes_consumed = in.bytes_consumed()
                                        - initial_bytes_consumed;
    if (unlikely(total_bytes_consumed != kafka::internal::kafka_header_size)) {
        throw std::runtime_error(fmt::format(
          "Invalid kafka header parsing. Must consume exactly:{}, but "
          "consumed:{}",
          kafka::internal::kafka_header_size,
          total_bytes_consumed));
    }
    return record_batch_info{
      .base_offset = base_offset,
      .record_count = record_count,
      .size_bytes = size_bytes};
}

} // namespace

model::offset consumer_records::last_offset() const {
    if (empty()) {
        return model::offset{-1};
    }
    iobuf_const_parser p{*_record_set};
    record_batch_info rbi{};
    // This is expected to be fast, but if there are stalls, it'll have to be
    // futurized.
    while (p.bytes_left()) {
        rbi = read_record_batch_info(p);
        p.skip(rbi.record_bytes());
    }
    return rbi.last_offset();
}

kafka_batch_adapter consumer_records::consume_record_batch() {
    iobuf_const_parser p{*_record_set};
    const auto hdr = read_record_batch_info(p);
    const auto size_bytes = hdr.size_bytes;
    kafka_batch_adapter kba;
    kba.adapt(_record_set->share(0, size_bytes));
    _record_set->trim_front(size_bytes);
    return kba;
}

} // namespace kafka
