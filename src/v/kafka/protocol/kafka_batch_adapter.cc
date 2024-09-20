// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/kafka_batch_adapter.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "compression/compression.h"
#include "hashing/crc32c.h"
#include "kafka/protocol/legacy_message.h"
#include "kafka/protocol/wire.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "storage/parser_utils.h"

#include <seastar/core/smp.hh>

#include <fmt/ostream.h>

#include <stdexcept>

namespace kafka {

model::record_batch_header kafka_batch_adapter::read_header(iobuf_parser& in) {
    const size_t initial_bytes_consumed = in.bytes_consumed();

    auto base_offset = model::offset(in.consume_be_type<int64_t>());
    auto batch_length = in.consume_be_type<int32_t>();
    auto partition_leader_epoch
      = in.consume_be_type<int32_t>();      /*partition_leader_epoch*/
    auto magic = in.consume_type<int8_t>(); /*magic - IGNORED*/
    v2_format = magic == 2;
    if (unlikely(!v2_format)) {
        return {}; // verified  in the adapt() call
    }
    auto crc = in.consume_be_type<uint32_t>();

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
      = batch_length - static_cast<int32_t>(internal::kafka_header_size)
        + static_cast<int32_t>(
          model::packed_record_batch_header_size
          // Kafka *does not* include the first 2 fields in the size calculation
          // they build the types bottoms up, not top down
          + sizeof(base_offset) + sizeof(batch_length));

    auto record_count = in.consume_be_type<int32_t>();
    auto header = model::record_batch_header{
      .size_bytes = size_bytes,
      .base_offset = base_offset,
      .type = model::record_batch_type::raft_data,
      .crc = crc,
      .attrs = attrs,
      .last_offset_delta = last_offset_delta,
      .first_timestamp = first_timestamp,
      .max_timestamp = max_timestamp,
      .producer_id = producer_id,
      .producer_epoch = producer_epoch,
      .base_sequence = base_sequence,
      .record_count = record_count};

    // see record_batch_header::context for more info

    header.ctx.term = model::term_id(partition_leader_epoch);

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

void kafka_batch_adapter::verify_crc(
  const model::record_batch_header& header, iobuf_parser in) {
    auto crc = crc::crc32c();

    // move the cursor to correct offset where the data to be checksummed
    // begins. That location skips the following 21 bytes:
    //
    //   - 8 base offset
    //   - 4 batch length
    //   - 4 partition leader epoch
    //   - 1 magic
    //   - 4 expected crc
    //
    auto total_bytes = in.bytes_left();
    static constexpr size_t checksum_data_offset_start = 21;
    in.skip(checksum_data_offset_start);

    // 2. consume & checksum the CRC
    auto consumed_bytes = in.consume(
      in.bytes_left(), [&crc](const char* src, size_t n) {
          // NOLINTNEXTLINE
          crc.extend(reinterpret_cast<const uint8_t*>(src), n);
          return ss::stop_iteration::no;
      });

    if (unlikely(header.crc != crc.value())) {
        valid_crc = false;
        vlog(
          klog.warn,
          "Cannot validate Kafka record batch {}. Mismatching CRC {}, "
          "checksummed bytes {}, total bytes: {}",
          header,
          crc.value(),
          consumed_bytes,
          total_bytes);
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
    auto trimmed_bytes = remainder.size_bytes();
    kbatch.trim_back(trimmed_bytes);

    auto parser = iobuf_parser(kbatch.share(0, kbatch.size_bytes()));
    auto header = read_header(parser);
    if (unlikely(!v2_format)) {
        vlog(
          klog.error,
          "can only parse magic.version2 format messages. ignoring");
        return remainder;
    }

    auto crcparser = iobuf_parser(std::move(kbatch));
    verify_crc(header, std::move(crcparser));
    if (unlikely(!valid_crc)) {
        if (trimmed_bytes > 0) {
            vlog(
              klog.info,
              "Trimmed bytes {} from batch: {}",
              trimmed_bytes,
              header);
        }
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

/*
 * Handle a MessageSet. For each uncompressed message the message data is
 * accumulated in the new-style redpanda batch format. Compressed messages
 * contain a nested MessageSet which is handled recursively (nesting should not
 * exceed one level).
 *
 * The resulting record batch is compressed if any of the individual message
 * sets are compressed.  In this case the compression type used is the last
 * compression type encountered.
 *
 * The last message determines the timestamp used on the resulting record batch.
 *
 * TODO
 *   - https://github.com/redpanda-data/redpanda/issues/1396
 */
void kafka_batch_adapter::convert_message_set(
  storage::record_batch_builder& builder,
  iobuf kbatch,
  bool expect_uncompressed) {
    auto parser = iobuf_parser(std::move(kbatch));
    while (parser.bytes_left()) {
        auto batch = decode_legacy_batch(parser);
        if (!batch) {
            // decoder logs more detail
            legacy_error = true;
            return;
        }

        if (batch->timestamp) {
            builder.set_timestamp(*batch->timestamp);
        }

        /*
         * if no compression then take the key/value and move on
         */
        if (batch->compression() == model::compression::none) {
            builder.add_raw_kv(std::move(batch->key), std::move(batch->value));
            continue;
        }

        /*
         * lz4 with magic_0 was implemented in kafka with a bug and requires
         * special handling of the encoding/decoding step. we can build a
         * variant that is compliant if needed, but at least one client (sarama)
         * simply disallow lz4 for versions that require magic_0.
         */
        if (
          batch->magic == 0
          && batch->compression() == model::compression::lz4) {
            vlog(
              klog.error,
              "Producing with magic=0 and lz4 compression is not supported");
            legacy_error = true;
            return;
        }

        builder.set_compression(batch->compression());
        if (expect_uncompressed) {
            vlog(klog.error, "MessageSet contains more than one nesting level");
            legacy_error = true;
            return;
        }

        if (!batch->value) {
            vlog(klog.error, "Compressed legacy does not contain a value");
            legacy_error = true;
            return;
        }

        auto batch_data = compression::compressor::uncompress(
          *batch->value, batch->compression());

        convert_message_set(builder, std::move(batch_data), true);
    }
}

void kafka_batch_adapter::adapt_with_version(
  iobuf kbatch, api_version version) {
    if (version >= api_version(3)) {
        adapt(std::move(kbatch));
        return;
    }

    // accumulates records from legacy message set
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    convert_message_set(builder, std::move(kbatch), false);

    if (legacy_error) {
        return;
    }

    // make produce handler happy: it validates that it is receiving a v2
    // formatted batch. since we are translating transparently we'll lie.
    v2_format = true;

    // trivially true since we computed it
    valid_crc = true;

    batch = std::move(builder).build();
}

} // namespace kafka
