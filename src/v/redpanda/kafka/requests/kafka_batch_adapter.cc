#include "redpanda/kafka/requests/kafka_batch_adapter.h"

namespace kafka::requests {

static constexpr model::record_batch_type kafka_record_batch_type() {
    return model::record_batch_type(1);
}

struct header_and_kafka_size {
    model::record_batch_header header;
    size_t kafka_size;
};

static header_and_kafka_size read_header(fragbuf::istream& in) {
    auto base_offset = model::offset(in.read<int64_t>());
    auto batch_length = in.read<uint32_t>();
    in.skip(sizeof(int32_t)); // partition leader epoch
    auto magic = in.read<int8_t>();
    if (magic != 2) {
        throw std::runtime_error("Unsupported Kafka batch format");
    }
    auto crc = in.read<int32_t>();
    auto attrs = model::record_batch_attributes(in.read<int16_t>());
    auto last_offset_delta = in.read<int32_t>();
    auto first_timestamp = model::timestamp(in.read<int64_t>());
    auto max_timestamp = model::timestamp(in.read<int64_t>());
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
read_records(fragbuf::istream& in, size_t num_records) {
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(num_records);
    for (unsigned i = 0; i < num_records; ++i) {
        auto [length, length_size] = vint::deserialize(in);
        in.skip(length_size);
        in.skip(sizeof(int8_t)); // unused attributes
        auto [timestamp_delta, timestamp_delta_size] = vint::deserialize(in);
        in.skip(timestamp_delta_size);
        auto [offset_delta, offset_delta_size] = vint::deserialize(in);
        in.skip(offset_delta_size);
        auto [key_length, key_length_size] = vint::deserialize(in);
        in.skip(key_length_size);
        auto key = in.read_shared(key_length);
        auto value_and_headers_size = length - length_size - sizeof(int8_t)
                                      - timestamp_delta_size - offset_delta_size
                                      - key_length - key_length_size;
        rs.emplace_back(
          length
            - 1, // FIXME: Remove when we add the attributes byte to the record.
          timestamp_delta,
          offset_delta,
          std::move(key),
          in.read_shared(value_and_headers_size));
    }
    return rs;
}

model::record_batch_reader reader_from_kafka_batch(fragbuf&& kafka_batch) {
    auto in = kafka_batch.get_istream();
    std::vector<model::record_batch> ret;
    while (in.bytes_left()) {
        auto [header, batch_length] = read_header(in);
        model::record_batch::records_type records;
        auto num_records = in.read<int32_t>();
        if (header.attrs.compression() != model::compression::none) {
            auto records_size = batch_length - internal::kafka_header_overhead;
            records = model::record_batch::compressed_records(
              num_records, in.read_shared(records_size));
        } else {
            // FIXME: Remove when we add the attributes byte to the record.
            header.size_bytes -= sizeof(int8_t) * num_records;
            records = read_records(in, num_records);
        }
        ret.emplace_back(std::move(header), std::move(records));
    }
    return model::make_memory_record_batch_reader(std::move(ret));
}

} // namespace kafka::requests
