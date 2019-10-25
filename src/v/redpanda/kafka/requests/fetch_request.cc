#include "redpanda/kafka/requests/fetch_request.h"

#include "redpanda/kafka/errors/errors.h"
#include "storage/tests/random_batch.h"

#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

class kafka_batch_serializer {
public:
    kafka_batch_serializer() noexcept
      : _wr(_buf) {
    }

    kafka_batch_serializer(const kafka_batch_serializer& o) noexcept = delete;
    kafka_batch_serializer& operator=(const kafka_batch_serializer& o) noexcept
      = delete;
    kafka_batch_serializer& operator=(kafka_batch_serializer&& o) noexcept
      = delete;

    kafka_batch_serializer(kafka_batch_serializer&& o) noexcept
      : _buf(std::move(o._buf))
      , _wr(_buf) {
    }

    future<stop_iteration> operator()(model::record_batch&& batch) {
        if (batch.compressed()) {
            // skip. the random batch maker doesn't yet create valid compressed
            // kafka records.
        } else {
            write_batch(std::move(batch));
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }

    bytes_ostream end_of_stream() {
        return std::move(_buf);
    }

private:
    void write_batch(model::record_batch&& batch) {
        // adjust the batch size to match the kafka wire size
        auto size = batch.size_bytes()
                    - sizeof(model::record_batch_header::base_offset)
                    + sizeof(int32_t)  // partition leader epoch
                    + sizeof(int8_t)   // magic
                    + sizeof(int64_t)  // producer id
                    + sizeof(int16_t)  // producer epoch
                    + sizeof(int32_t); // base sequence

        _wr.write(int64_t(batch.base_offset().value()));
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
            write_record(std::move(record));
        }
    }

    void write_record(model::record&& record) {
        _wr.write_varint(record.size_bytes());
        _wr.write(int8_t(0));
        _wr.write_varint(record.timestamp_delta());
        _wr.write_varint(record.offset_delta());
        _wr.write_varint(record.key().size_bytes());
        _wr.write_direct(record.release_key());
        _wr.write_direct(record.release_packed_value_and_headers());
    }

private:
    bytes_ostream _buf;
    response_writer _wr;
};

struct partition {
    model::partition_id id;
    model::offset offset;
    int32_t max_bytes;
};

struct topic {
    model::topic name;
    std::vector<partition> partitions;
};

future<response_ptr>
fetch_api::process(request_context&& ctx, smp_service_group g) {
    // TODO: don't use seastar threads for performance reasons
    return async([ctx = std::move(ctx)]() mutable {
        auto replica_id = ctx.reader().read_int32();
        auto max_wait_time = ctx.reader().read_int32();
        auto min_bytes = ctx.reader().read_int32();
        auto max_bytes = ctx.reader().read_int32();
        auto isolation_level = ctx.reader().read_int8();
        auto topics = ctx.reader().read_array([&ctx](request_reader& r) {
            model::topic name(r.read_string());
            auto partitions = r.read_array([&ctx](request_reader& r) {
                model::partition_id id(r.read_int32());
                model::offset offset(r.read_int64());
                auto max_bytes = r.read_int32();
                return partition{id, offset, max_bytes};
            });
            return topic{name, partitions};
        });

        // TODO: for each partition we'll be reading from, go ahead and setup
        // the readers. for each partition/reader initializion, seeking, cache
        // warming, etc... could all be done asynchronously.

        auto resp = std::make_unique<response>();

        resp->writer().write(int32_t(0));
        resp->writer().write_array(
          topics, [&ctx](const auto& topic, response_writer& wr) {
              wr.write(topic.name);
              wr.write_array(
                topic.partitions,
                [&ctx](const auto& partition, response_writer& wr) {
                    wr.write(partition.id);
                    wr.write(errors::error_code::none);
                    wr.write(int64_t(0));
                    wr.write(int64_t(0));
                    wr.write_array(
                      std::vector<char>{}, [](char c, response_writer& wr) {});

                    auto batches = storage::test::make_random_batches(
                      model::offset(partition.offset), 5);
                    auto reader = model::make_memory_record_batch_reader(
                      std::move(batches));

                    // in general the serializer will run on a different core
                    // and we'll collect from each core the underlying
                    // bytes_ostream buffers to build the response.
                    // TODO: need to add a more convenient interface on the
                    // response writer but its not yet clear how many different
                    // versions we'll need. So this is a bit verbose right now.
                    auto batch = reader
                                   .consume(
                                     kafka_batch_serializer(),
                                     model::no_timeout)
                                   .get0();
                    auto size = wr.write_bytes_wrapped(
                      [batch = std::move(batch)](response_writer& wr) mutable {
                          wr.write_direct(std::move(batch));
                          return false;
                      });
                });
          });

        return response_ptr(std::move(resp));
    });
}
} // namespace kafka::requests
