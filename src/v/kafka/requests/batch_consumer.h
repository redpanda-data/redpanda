#pragma once
#include "bytes/iobuf.h"
#include "kafka/requests/response_writer.h"
#include "model/record.h"

namespace kafka {

/**
 * A record batch reader consumer that serializes a stream of batches to the
 * Kafka on-wire format. The primary use case for this is the fetch api which
 * returns a set of batches read from a redpanda log back to a kafka client.
 */
class kafka_batch_serializer {
public:
    kafka_batch_serializer() noexcept
      : _wr(_buf) {}

    kafka_batch_serializer(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(kafka_batch_serializer&& o) = delete;

    kafka_batch_serializer(kafka_batch_serializer&& o) noexcept
      : _buf(std::move(o._buf))
      , _wr(_buf) {}

    future<stop_iteration> operator()(model::record_batch&& batch) {
        if (batch.compressed()) {
            // skip. the random batch maker doesn't yet create valid
            // compressed kafka records.
        } else {
            write_batch(std::move(batch));
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }

    iobuf end_of_stream() { return std::move(_buf); }

private:
    void write_batch(model::record_batch&& batch) {
        // adjust the batch size to match the kafka wire size
        auto size = batch.size_bytes()
                    - sizeof(model::record_batch_header::base_offset)
                    - sizeof(model::record_batch_type::type)
                    + sizeof(int32_t)  // partition leader epoch
                    + sizeof(int8_t)   // magic
                    + sizeof(int64_t)  // producer id
                    + sizeof(int16_t)  // producer epoch
                    + sizeof(int32_t); // base sequence

        _wr.write(int64_t(batch.base_offset()));
        _wr.write(int32_t(size)); // batch length
        _wr.write(int32_t(0));    // partition leader epoch
        _wr.write(int8_t(2));     // magic
        _wr.write(batch.crc());   // crc
        _wr.write(int16_t(
          batch.attributes().value())); // attributes (fixed to no compression)
        _wr.write(int32_t(batch.last_offset_delta()));
        _wr.write(int64_t(batch.first_timestamp().value()));
        _wr.write(int64_t(batch.max_timestamp().value()));
        _wr.write(int64_t(0));            // producer id
        _wr.write(int16_t(0));            // producer epoch
        _wr.write(int32_t(0));            // base sequence
        _wr.write(int32_t(batch.size())); // num records

        for (auto& record : batch) {
            _wr.write_varint(record.size_bytes());
            _wr.write(int8_t(0));
            _wr.write_varint(record.timestamp_delta());
            _wr.write_varint(record.offset_delta());
            _wr.write_varint(record.key().size_bytes());
            _wr.write_direct(record.share_key());
            _wr.write_direct(record.share_packed_value_and_headers());
        }
    }

private:
    iobuf _buf;
    response_writer _wr;
};

} // namespace kafka
