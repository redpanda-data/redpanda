#include "kafka/requests/kafka_batch_adapter.h"

#include "kafka/requests/request_context.h"
#include "kafka/requests/request_reader.h"
#include "raft/types.h"

namespace kafka {

struct header_and_kafka_size {
    model::record_batch_header header;
    size_t kafka_size;
};

static vint::result consume_vint(iobuf::iterator_consumer& in) {
    auto [value, size] = vint::deserialize(in);
    in.skip(size);
    return {value, size};
}

static header_and_kafka_size read_header(iobuf::iterator_consumer& in) {
    auto base_offset = model::offset(request_reader::_read_int64(in));
    auto batch_length = request_reader::_read_int32(in);
    in.skip(sizeof(int32_t)); // partition leader epoch
    auto magic = in.consume_type<int8_t>();
    if (magic != 2) {
        throw invalid_record_exception(
          fmt::format("unsupported kafka batch magic={}", magic));
    }
    auto crc = request_reader::_read_int32(in);

    auto attrs = model::record_batch_attributes(
      request_reader::_read_int16(in));
    if (attrs.is_transactional()) {
        throw invalid_record_exception("transactional records not supported");
    }

    auto last_offset_delta = request_reader::_read_int32(in);
    auto first_timestamp = model::timestamp(request_reader::_read_int64(in));
    auto max_timestamp = model::timestamp(request_reader::_read_int64(in));

    auto producer_id = request_reader::_read_int64(in);
    if (producer_id >= 0) {
        throw invalid_record_exception(fmt::format(
          "idempotent records not supported producer id {}", producer_id));
    }

    in.skip(sizeof(int16_t) + sizeof(int32_t)); // producer epoch, base sequence
    uint32_t size_bytes = batch_length - internal::kafka_header_overhead;
    auto header = model::record_batch_header{
      .size_bytes = size_bytes,
      .base_offset = base_offset,
      .type = raft::data_batch_type,
      .crc = crc,
      .attrs = attrs,
      .last_offset_delta = last_offset_delta,
      .first_timestamp = first_timestamp,
      .max_timestamp = max_timestamp,
    };
    return {std::move(header), (size_t)batch_length};
}

static model::record_batch::uncompressed_records
read_records(iobuf::iterator_consumer& in, iobuf& b, size_t num_records) {
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(num_records);
    for (unsigned i = 0; i < num_records; ++i) {
        auto [length, length_size] = consume_vint(in);
        auto attributes = model::record_attributes(in.consume_type<int8_t>());
        auto [timestamp_delta, timestamp_delta_size] = consume_vint(in);
        auto [offset_delta, offset_delta_size] = consume_vint(in);
        auto [key_length, key_length_size] = consume_vint(in);

        iobuf key;
        if (key_length >= 0) {
            key = b.share(in.bytes_consumed(), key_length);
        } else {
            key_length = 0;
        }
        in.skip(key_length);

        auto value_and_headers_size = length - sizeof(int8_t)
                                      - timestamp_delta_size - offset_delta_size
                                      - key_length - key_length_size;
        auto val_and_headers = b.share(
          in.bytes_consumed(), value_and_headers_size);
        in.skip(value_and_headers_size);

        rs.emplace_back(
          length,
          attributes,
          timestamp_delta,
          offset_delta,
          std::move(key),
          std::move(val_and_headers));
    }
    return rs;
}

model::record_batch_reader reader_from_kafka_batch(iobuf&& kbatch) {
    auto in = iobuf::iterator_consumer(kbatch.cbegin(), kbatch.cend());
    std::vector<model::record_batch> ret;
    while (!in.is_finished()) {
        if (!ret.empty()) {
            // produce >= v3
            throw invalid_record_exception("produce requests are required to "
                                           "contain exactly one record batch");
        }

        auto [header, batch_length] = read_header(in);
        auto num_records = request_reader::_read_int32(in);

        model::record_batch::records_type records;
        if (header.attrs.compression() != model::compression::none) {
            auto records_size = batch_length - internal::kafka_header_overhead;
            auto buf = kbatch.share(in.bytes_consumed(), records_size);
            in.skip(records_size);
            records = model::record_batch::compressed_records(
              num_records, std::move(buf));
        } else {
            records = read_records(in, kbatch, num_records);
        }
        ret.emplace_back(std::move(header), std::move(records));
    }
    return model::make_memory_record_batch_reader(std::move(ret));
}

} // namespace kafka
