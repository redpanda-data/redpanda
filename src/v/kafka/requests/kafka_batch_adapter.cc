#include "kafka/requests/kafka_batch_adapter.h"

#include "hashing/crc32c.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/request_reader.h"
#include "likely.h"
#include "model/record.h"
#include "raft/types.h"
#include "vassert.h"

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
    has_non_v2_magic = has_non_v2_magic || magic != 2;
    auto crc = in.consume_be_type<int32_t>();

    auto attrs = model::record_batch_attributes(in.consume_be_type<int16_t>());
    has_transactional = has_transactional || attrs.is_transactional();

    auto last_offset_delta = in.consume_be_type<int32_t>();
    auto first_timestamp = model::timestamp(in.consume_be_type<int64_t>());
    auto max_timestamp = model::timestamp(in.consume_be_type<int64_t>());
    auto producer_id = in.consume_be_type<int64_t>();
    has_idempotent = has_idempotent || producer_id >= 0;
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

static void verify_crc(int32_t expected_crc, iobuf_parser in) {
    constexpr size_t bytes_to_skip = 13;

    auto crc = crc32();

    // 1. move the cursor to correct endpoint

    in.consume_be_type<int64_t>();                     /*base_offset*/
    auto batch_length = in.consume_be_type<int32_t>(); /*needed for crc*/
    in.consume_be_type<int32_t>(); /*partition_leader_epoch*/
    in.consume_type<int8_t>();     /*magic*/
    in.consume_be_type<int32_t>(); /*crc*/

    // 2. consume & checksum the CRC
    const auto consumed = in.consume(
      in.bytes_left(), [&crc](const char* src, size_t n) {
          // NOLINTNEXTLINE
          crc.extend(reinterpret_cast<const uint8_t*>(src), n);
          return ss::stop_iteration::no;
      });
    if (unlikely(expected_crc != crc.value())) {
        throw std::runtime_error(fmt::format(
          "Cannot validate Kafka record batch. Missmatching CRC. Expected:{}, "
          "Got:{}",
          expected_crc,
          crc.value()));
    }
}

void kafka_batch_adapter::adapt(iobuf&& kbatch) {
    auto crcparser = iobuf_parser(kbatch.control_share());
    auto parser = iobuf_parser(kbatch.control_share());
    auto header = read_header(parser);

    verify_crc(header.crc, std::move(crcparser));
    model::record_batch::records_type records;
    if (header.attrs.compression() != model::compression::none) {
        auto records_size = header.size_bytes
                            - model::packed_record_batch_header_size;
        records = parser.share(records_size);
    } else {
        records = read_records(parser, header.record_count);
    }
    batches.emplace_back(header, std::move(records));

    // Kafka guarantees - exactly one batch on the wire
    if (unlikely(parser.bytes_left())) {
        throw std::runtime_error(fmt::format(
          "Could not consume full record_batch. Bytes left on the wire:{}",
          parser.bytes_left()));
    }
}

} // namespace kafka
