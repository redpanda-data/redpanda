#include "kafka/requests/kafka_batch_adapter.h"

namespace kafka {

static constexpr model::record_batch_type kafka_record_batch_type() {
    return model::record_batch_type(1);
}

struct header_and_kafka_size {
    model::record_batch_header header;
    size_t kafka_size;
};

static header_and_kafka_size read_header(iobuf::iterator_consumer& in) {
    auto base_offset = model::offset(in.consume_type<int64_t>());
    auto batch_length = in.consume_type<uint32_t>();
    in.skip(sizeof(int32_t)); // partition leader epoch
    auto magic = in.consume_type<int8_t>();
    if (magic != 2) {
        throw std::runtime_error("Unsupported Kafka batch format");
    }
    auto crc = in.consume_type<int32_t>();
    auto attrs = model::record_batch_attributes(in.consume_type<int16_t>());
    auto last_offset_delta = in.consume_type<int32_t>();
    auto first_timestamp = model::timestamp(in.consume_type<int64_t>());
    auto max_timestamp = model::timestamp(in.consume_type<int64_t>());
    in.skip(
      sizeof(int64_t) + sizeof(int16_t)
      + sizeof(int32_t)); // producer id, producer epoch, base sequence
    uint32_t size_bytes = batch_length - internal::kafka_header_overhead;
    auto header = model::record_batch_header{size_bytes,
                                             base_offset,
                                             kafka_record_batch_type(),
                                             crc,
                                             attrs,
                                             last_offset_delta,
                                             first_timestamp,
                                             max_timestamp};
    return {std::move(header), batch_length};
}

static model::record_batch::uncompressed_records
read_records(iobuf::iterator_consumer& in, iobuf& b, size_t num_records) {
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(num_records);
    for (unsigned i = 0; i < num_records; ++i) {
        auto [length, length_size] = vint::deserialize(in);
        in.skip(length_size);
        auto attributes = model::record_attributes(in.consume_type<int8_t>());
        auto [timestamp_delta, timestamp_delta_size] = vint::deserialize(in);
        in.skip(timestamp_delta_size);
        auto [offset_delta, offset_delta_size] = vint::deserialize(in);
        in.skip(offset_delta_size);
        auto [key_length, key_length_size] = vint::deserialize(in);
        in.skip(key_length_size);
        auto key = b.share(in.bytes_consumed(), key_length);
        in.skip(key_length);
        auto value_and_headers_size = length - length_size - sizeof(int8_t)
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
        auto [header, batch_length] = read_header(in);
        model::record_batch::records_type records;
        auto num_records = in.consume_type<int32_t>();
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
