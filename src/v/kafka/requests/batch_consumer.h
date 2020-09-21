#pragma once
#include "bytes/iobuf.h"
#include "kafka/requests/kafka_batch_adapter.h"
#include "kafka/requests/response_writer.h"
#include "kafka/requests/response_writer_utils.h"
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
    struct result {
        iobuf data;
        uint32_t record_count;
    };

    kafka_batch_serializer() noexcept
      : _wr(_buf) {}

    kafka_batch_serializer(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(kafka_batch_serializer&& o) = delete;

    kafka_batch_serializer(kafka_batch_serializer&& o) noexcept
      : _buf(std::move(o._buf))
      , _wr(_buf) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        record_count_ += batch.record_count();
        write_batch(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    result end_of_stream() {
        return result{
          .data = std::move(_buf),
          .record_count = record_count_,
        };
    }

private:
    void write_batch(model::record_batch&& batch) {
        writer_serialize_batch(_wr, std::move(batch));
    }

private:
    iobuf _buf;
    response_writer _wr;
    uint32_t record_count_ = 0;
};

} // namespace kafka
