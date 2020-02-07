#pragma once
#include "bytes/iobuf.h"
#include "kafka/requests/kafka_batch_adapter.h"
#include "kafka/requests/response_writer.h"
#include "model/record.h"
#include "seastarx.h"

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

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        write_batch(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    iobuf end_of_stream() { return std::move(_buf); }

private:
    void write_batch(model::record_batch&& batch) {
        /*
         * calculate batch size expected by kafka client.
         *
         * 1. records_size = batch.size_bytes() - RP header size;
         * 2. kafka_total = records_size + kafka header size;
         * 3. batch_size = kafka_total - sizeof(offset) - sizeof(length);
         *
         * The records size in (1) is computed correctly because RP batch size
         * is defined as the RP header size plus the size of the records. Unlike
         * the kafka batch size described below, RP batch size includes the size
         * of the length field itself.
         *
         * The adjustment in (3) is because the batch size given in the kafka
         * header does not include the offset preceeding the length field nor
         * the size of the length field itself.
         */
        auto size = batch.size_bytes() - model::packed_record_batch_header_size
                    + internal::kafka_header_size - sizeof(int64_t)
                    - sizeof(int32_t);

        _wr.write(int64_t(batch.base_offset()));
        _wr.write(int32_t(size)); // batch length
        _wr.write(int32_t(0));    // partition leader epoch
        _wr.write(int8_t(2));     // magic
        _wr.write(batch.header().crc);
        _wr.write(int16_t(batch.header().attrs.value()));
        _wr.write(int32_t(batch.header().last_offset_delta));
        _wr.write(int64_t(batch.header().first_timestamp.value()));
        _wr.write(int64_t(batch.header().max_timestamp.value()));
        _wr.write(int64_t(batch.header().producer_id));
        _wr.write(int16_t(batch.header().producer_epoch));
        _wr.write(int32_t(batch.header().base_sequence));
        _wr.write(int32_t(batch.record_count()));

        if (batch.compressed()) {
            _wr.write(std::move(batch).release());
        } else {
            for (auto& record : batch) {
                _wr.write_varint(record.size_bytes());
                _wr.write(int8_t(0));
                _wr.write_varint(record.timestamp_delta());
                _wr.write_varint(record.offset_delta());
                _wr.write_varint(record.key_size());
                _wr.write_direct(record.share_key());
                _wr.write_varint(record.value_size());
                _wr.write_direct(record.share_value());
                _wr.write_varint(record.headers().size());
                for (auto& h : record.headers()) {
                    _wr.write_varint(h.key_size());
                    _wr.write_direct(h.share_key());
                    _wr.write_varint(h.value_size());
                    _wr.write_direct(h.share_value());
                }
            }
        }
    }

private:
    iobuf _buf;
    response_writer _wr;
};

} // namespace kafka
