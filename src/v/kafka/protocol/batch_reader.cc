// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/batch_reader.h"

#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace kafka {

namespace {

/// \brief A subset of the model::record_batch_header
///
/// Just enough to delineate record_batch within consumer_records.
class record_batch_info {
public:
    record_batch_info(model::offset bo, int32_t lod, int32_t sb)
      : _base_offset{bo}
      , _last_offset_delta{lod}
      , _size_bytes{sb} {}

    int32_t size_bytes() const { return _size_bytes; }
    int32_t record_bytes() const {
        return _size_bytes
               - static_cast<int32_t>(model::packed_record_batch_header_size);
    }
    model::offset last_offset() const {
        return _base_offset + model::offset{_last_offset_delta};
    }

private:
    model::offset _base_offset;
    int32_t _last_offset_delta;
    int32_t _size_bytes;
};

record_batch_info read_record_batch_info(iobuf_const_parser& in) {
    if (unlikely(in.bytes_left() < internal::kafka_header_size)) {
        throw exception(
          error_code::corrupt_message,
          fmt_with_ctx(
            fmt::format,
            "Invalid kafka header parsing: Only {} bytes remain",
            in.bytes_left()));
    }
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
                                  sizeof(int32_t) + // base_sequence
                                  sizeof(int32_t);  // record_count
    in.skip(skip_len_2);

    // size_bytes are  the normal kafka batch length minus the `IGNORED`
    const int32_t size_bytes
      = batch_length - internal::kafka_header_size
        + model::packed_record_batch_header_size
        // Kafka *does not* include the first 2 fields in the size calculation
        // they build the types bottoms up, not top down
        + sizeof(base_offset) + sizeof(batch_length);

    const size_t total_bytes_consumed = in.bytes_consumed()
                                        - initial_bytes_consumed;
    if (unlikely(total_bytes_consumed != internal::kafka_header_size)) {
        throw exception(
          error_code::corrupt_message,
          fmt_with_ctx(
            fmt::format,
            "Invalid kafka header parsing. Must consume exactly: {} bytes, but "
            "consumed: {} bytes",
            internal::kafka_header_size,
            total_bytes_consumed));
    }
    return record_batch_info{base_offset, last_offset_delta, size_bytes};
}

} // namespace

model::offset batch_reader::last_offset() const {
    iobuf_const_parser p{_buf};
    model::offset last_offset{0};
    // Complexity: O(n) record_batches
    // This should be futurized if there are stalls.
    while (p.bytes_left()) {
        const auto rbi = read_record_batch_info(p);
        last_offset = rbi.last_offset();
        p.skip(rbi.record_bytes());
    }
    return last_offset;
}

kafka_batch_adapter batch_reader::consume_batch() {
    iobuf_const_parser p{_buf};
    const auto hdr = read_record_batch_info(p);
    const auto size_bytes = hdr.size_bytes();
    kafka_batch_adapter kba;
    kba.adapt(_buf.share(0, size_bytes));
    _buf.trim_front(size_bytes);
    return kba;
}

ss::future<batch_reader::storage_t>
batch_reader::do_load_slice(model::timeout_clock::time_point tp) {
    using data_t = model::record_batch_reader::data_t;
    return ss::do_with(data_t{}, [this, tp](data_t& batches) {
        const auto resources_exceeded = [this, tp] {
            return is_end_of_stream() || model::timeout_clock::now() > tp;
        };
        const auto consume_one = [this, &batches]() {
            auto kba = consume_batch();
            if (likely(kba.v2_format && kba.valid_crc && kba.batch)) {
                batches.push_back(std::move(*kba.batch));
                return ss::now();
            } else {
                _do_load_slice_failed = true;
                batches.clear();

                const auto msg = [&kba] {
                    if (kba.v2_format) {
                        if (kba.valid_crc) {
                            return "empty batch";
                        }
                        return "invalid crc";
                    }
                    return "not v2_format";
                }();

                return ss::make_exception_future<>(exception(
                  kafka::error_code::corrupt_message,
                  fmt_with_ctx(
                    fmt::format, "Invalid kafka record parsing: {}", msg)));
            }
        };
        return ss::do_until(resources_exceeded, consume_one).then([&batches]() {
            return storage_t(std::move(batches));
        });
    });
}

} // namespace kafka
