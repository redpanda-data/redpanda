// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/kafka_batch_adapter.h"

#include "bytes/iobuf.h"
#include "hashing/crc32c.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/request_reader.h"
#include "likely.h"
#include "model/record.h"
#include "raft/types.h"
#include "storage/parser_utils.h"
#include "vassert.h"

#include <seastar/core/smp.hh>

#include <stdexcept>

namespace kafka {

model::record_batch_header kafka_batch_adapter::read_header(iobuf_parser& in) {
    const size_t initial_bytes_consumed = in.bytes_consumed();

    auto base_offset = model::offset(in.consume_be_type<int64_t>());
    auto batch_length = in.consume_be_type<int32_t>();
    in.consume_be_type<int32_t>();          /*partition_leader_epoch - IGNORED*/
    auto magic = in.consume_type<int8_t>(); /*magic - IGNORED*/
    v2_format = magic == 2;
    if (unlikely(!v2_format)) {
        return {}; // verified  in the adapt() call
    }
    auto crc = in.consume_be_type<int32_t>();

    auto attrs = model::record_batch_attributes(in.consume_be_type<int16_t>());

    auto last_offset_delta = in.consume_be_type<int32_t>();
    auto first_timestamp = model::timestamp(in.consume_be_type<int64_t>());
    auto max_timestamp = model::timestamp(in.consume_be_type<int64_t>());
    auto producer_id = in.consume_be_type<int64_t>();
    auto producer_epoch = in.consume_be_type<int16_t>();
    auto base_sequence = in.consume_be_type<int32_t>();

    // size_bytes are  the normal kafka batch length minus the `IGNORED` fields
    // note that we are wire compatible with Kafka with everything below the CRC
    // Note:
    //   if you change this, please remember to change batch_consumer.h
    int32_t size_bytes
      = batch_length - internal::kafka_header_size
        + model::packed_record_batch_header_size
        // Kafka *does not* include the first 2 fields in the size calculation
        // they build the types bottoms up, not top down
        + sizeof(base_offset) + sizeof(batch_length);

    auto record_count = in.consume_be_type<int32_t>();
    auto header = model::record_batch_header{
      .size_bytes = size_bytes,
      .base_offset = base_offset,
      .type = raft::data_batch_type,
      .crc = crc,
      .attrs = attrs,
      .last_offset_delta = last_offset_delta,
      .first_timestamp = first_timestamp,
      .max_timestamp = max_timestamp,
      .producer_id = producer_id,
      .producer_epoch = producer_epoch,
      .base_sequence = base_sequence,
      .record_count = record_count};

    const size_t total_bytes_consumed = in.bytes_consumed()
                                        - initial_bytes_consumed;
    if (unlikely(total_bytes_consumed != internal::kafka_header_size)) {
        throw std::runtime_error(fmt::format(
          "Invalid kafka header parsing. Must consume exactly:{}, but "
          "consumed:{}",
          internal::kafka_header_size,
          total_bytes_consumed));
    }
    header.ctx.owner_shard = ss::this_shard_id();
    return header;
}

void kafka_batch_adapter::verify_crc(int32_t expected_crc, iobuf_parser in) {
    auto crc = crc32();

    // 1. move the cursor to correct endpoint
    //   - 8 base offset
    //   - 4 batch length
    //   - 4 partition leader epoch
    //   - 1 magic
    //   - 4 exepcted crc
    in.skip(21);

    // 2. consume & checksum the CRC
    in.consume(in.bytes_left(), [&crc](const char* src, size_t n) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), n);
        return ss::stop_iteration::no;
    });

    // the crc is calculated over the bytes we receive as a uint32_t, but the
    // crc arrives off the wire as a signed 32-bit value.
    if (unlikely((uint32_t)expected_crc != crc.value())) {
        valid_crc = false;
        vlog(
          klog.error,
          "Cannot validate Kafka record batch. Missmatching CRC. Expected:{}, "
          "Got:{}",
          expected_crc,
          crc.value());
    } else {
        valid_crc = true;
    }
}

iobuf kafka_batch_adapter::adapt(iobuf&& kbatch) {
    // The batch size given in the kafka header does not include the offset
    // preceeding the length field nor the size of the length field itself.
    constexpr size_t kafka_length_diff
      = sizeof(model::record_batch_header::base_offset)
        + sizeof(model::record_batch_header::size_bytes);

    if (unlikely(kbatch.size_bytes() < kafka_length_diff)) {
        vlog(klog.error, "kbatch is unexpectedly small");
        return iobuf{};
    }

    auto batch_length =
      [peeker{iobuf_parser(kbatch.share(0, kafka_length_diff))}]() mutable {
          peeker.skip(sizeof(model::record_batch_header::base_offset));
          return peeker.consume_be_type<int32_t>() + kafka_length_diff;
      }();

    auto remainder = kbatch.share(
      batch_length, kbatch.size_bytes() - batch_length);
    kbatch.trim_back(remainder.size_bytes());

    auto crcparser = iobuf_parser(kbatch.share(0, kbatch.size_bytes()));
    auto parser = iobuf_parser(std::move(kbatch));

    auto header = read_header(parser);
    if (unlikely(!v2_format)) {
        vlog(
          klog.error,
          "can only parse magic.version2 format messages. ignoring");
        return remainder;
    }

    verify_crc(header.crc, std::move(crcparser));
    if (unlikely(!valid_crc)) {
        vlog(klog.error, "batch has invalid CRC: {}", header);
        return remainder;
    }

    auto records_size = header.size_bytes
                        - model::packed_record_batch_header_size;
    auto records = parser.share(records_size);

    auto new_batch = model::record_batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});

    /**
     * Perform some type of validation on the uncompressed input. In this case
     * we make sure that the records can be materialized but we avoid
     * re-encoding them using the lazy-record optimization.
     */
    if (!new_batch.compressed()) {
        try {
            new_batch.for_each_record([](model::record r) { (void)r; });
        } catch (const std::exception& e) {
            vlog(klog.error, "Parsing uncompressed records: {}", e.what());
            return remainder;
        }
    }

    batch = std::move(new_batch);
    return remainder;
}

} // namespace kafka
