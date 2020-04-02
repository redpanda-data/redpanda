#include "kafka/requests/kafka_batch_adapter.h"

#include "hashing/crc32c.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/request_reader.h"
#include "likely.h"
#include "model/record.h"
#include "raft/types.h"
#include "vassert.h"

#include <seastar/core/smp.hh>

#include <stdexcept>

namespace kafka {

struct header_and_kafka_size {
    model::record_batch_header header;
    size_t kafka_size;
};

model::record_batch_header kafka_batch_adapter::read_header(iobuf_parser& in) {
    const size_t initial_bytes_consumed = in.bytes_consumed();

    auto base_offset = model::offset(in.consume_be_type<int64_t>());
    auto batch_length = in.consume_be_type<int32_t>();
    in.consume_be_type<int32_t>();          /*partition_leader_epoch - IGNORED*/
    auto magic = in.consume_type<int8_t>(); /*magic - IGNORED*/
    v2_format = magic == 2;
    if (unlikely(!v2_format)) {
        return {};
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

static std::vector<model::record_header>
read_record_headers(iobuf_parser& parser, int32_t num_headers) {
    std::vector<model::record_header> headers;
    for (int i = 0; i < num_headers; ++i) {
        auto [key_length, kv] = parser.read_varlong();
        iobuf key;
        if (key_length > 0) {
            key = parser.share(key_length);
        }
        auto [val_length, vv] = parser.read_varlong();
        iobuf val;
        if (val_length > 0) {
            val = parser.share(val_length);
        }
        headers.emplace_back(model::record_header(
          key_length, std::move(key), val_length, std::move(val)));
    }
    return headers;
}

static model::record_batch::uncompressed_records
read_records(iobuf_parser& parser, size_t num_records) {
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(num_records);
    for (unsigned i = 0; i < num_records; ++i) {
        auto [length, length_size] = parser.read_varlong();
        auto attributes = model::record_attributes(
          parser.consume_type<int8_t>());
        auto [timestamp_delta, timestamp_delta_size] = parser.read_varlong();
        auto [offset_delta, offset_delta_size] = parser.read_varlong();
        auto [key_length, key_length_size] = parser.read_varlong();
        iobuf key;
        if (key_length > 0) {
            key = parser.share(key_length);
        }
        auto [val_length, val_length_size] = parser.read_varlong();
        iobuf val;
        if (val_length > 0) {
            val = parser.share(val_length);
        }
        auto [num_headers, _] = parser.read_varlong();
        auto headers = read_record_headers(parser, num_headers);
        rs.emplace_back(
          length,
          attributes,
          timestamp_delta,
          offset_delta,
          key_length,
          std::move(key),
          val_length,
          std::move(val),
          std::move(headers));
    }
    return rs;
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
    in.consume(
      in.bytes_left(), [&crc](const char* src, size_t n) {
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

void kafka_batch_adapter::adapt(iobuf&& kbatch) {
    auto crcparser = iobuf_parser(kbatch.share(0, kbatch.size_bytes()));
    auto parser = iobuf_parser(std::move(kbatch));

    auto header = read_header(parser);
    if (unlikely(!v2_format)) {
        return;
    }

    verify_crc(header.crc, std::move(crcparser));
    if (unlikely(!valid_crc)) {
        return;
    }

    model::record_batch::records_type records;
    if (header.attrs.compression() != model::compression::none) {
        auto records_size = header.size_bytes
                            - model::packed_record_batch_header_size;
        records = parser.share(records_size);
    } else {
        records = read_records(parser, header.record_count);
    }

    // Kafka guarantees - exactly one batch on the wire
    if (unlikely(parser.bytes_left())) {
        klog.error(
          "Could not consume full record_batch. Bytes left on the wire:{}",
          parser.bytes_left());
        return;
    }

    batch = model::record_batch(header, std::move(records));
}

} // namespace kafka
