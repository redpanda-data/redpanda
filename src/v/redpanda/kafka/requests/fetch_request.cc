#include "redpanda/kafka/requests/fetch_request.h"

#include "redpanda/kafka/errors/errors.h"
#include "storage/tests/random_batch.h"
#include "utils/to_string.h"

#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

void fetch_request::encode(
  const request_context& ctx, response_writer& writer) {
    auto version = ctx.header().version;

    writer.write(replica_id());
    writer.write(int32_t(max_wait_time.count()));
    writer.write(min_bytes);
    if (version >= api_version(3)) {
        writer.write(max_bytes);
    }
    if (version >= api_version(4)) {
        writer.write(isolation_level);
    }
    writer.write_array(
      topics, [version](const topic& t, response_writer& writer) {
          writer.write(t.name());
          writer.write_array(
            t.partitions,
            [version](const partition& p, response_writer& writer) {
                writer.write(p.id);
                writer.write(int64_t(p.fetch_offset));
                writer.write(p.partition_max_bytes);
            });
      });
}

void fetch_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    replica_id = model::node_id(reader.read_int32());
    max_wait_time = std::chrono::milliseconds(reader.read_int32());
    min_bytes = reader.read_int32();
    if (version >= api_version(3)) {
        max_bytes = reader.read_int32();
    }
    if (version >= api_version(4)) {
        isolation_level = reader.read_int8();
    }
    topics = reader.read_array([](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([](request_reader& reader) {
              struct partition p;
              p.id = model::partition_id(reader.read_int32());
              p.fetch_offset = model::offset(reader.read_int64());
              p.partition_max_bytes = reader.read_int32();
              return p;
          })};
    });
}

std::ostream& operator<<(std::ostream& o, const fetch_request::partition& p) {
    return fmt_print(
      o, "id {} off {} max {}", p.id, p.fetch_offset, p.partition_max_bytes);
}

std::ostream& operator<<(std::ostream& o, const fetch_request::topic& t) {
    return fmt_print(o, "name {} parts {}", t.name, t.partitions);
}

std::ostream& operator<<(std::ostream& o, const fetch_request& r) {
    return fmt_print(
      o,
      "replica {} max_wait_time {} min_bytes {} max_bytes {} isolation {} "
      "topics {}",
      r.replica_id,
      r.max_wait_time,
      r.min_bytes,
      r.max_bytes,
      r.isolation_level,
      r.topics);
}

void fetch_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle_time.count()));
    }

    writer.write_array(
      partitions, [version](partition& p, response_writer& writer) {
          writer.write(p.name);
          writer.write_array(
            p.responses,
            [version](partition_response& r, response_writer& writer) {
                writer.write(r.id);
                writer.write(r.error);
                writer.write(int64_t(r.high_watermark));
                if (version >= api_version(4)) {
                    writer.write(int64_t(r.last_stable_offset));
                    writer.write_array(
                      r.aborted_transactions,
                      [](
                        const aborted_transaction& t, response_writer& writer) {
                          writer.write(t.producer_id);
                          writer.write(int64_t(t.first_offset));
                      });
                }
                writer.write_bytes_wrapped(
                  [record_set = std::move(r.record_set)](
                    response_writer& writer) mutable {
                      writer.write_direct(std::move(record_set));
                      return false;
                  });
            });
      });
}

std::ostream&
operator<<(std::ostream& o, const fetch_response::aborted_transaction& t) {
    return fmt_print(
      o, "producer {} first_off {}", t.producer_id, t.first_offset);
}

std::ostream&
operator<<(std::ostream& o, const fetch_response::partition_response& p) {
    return fmt_print(
      o,
      "id {} err {} high_water {} last_stable_off {} aborted {} "
      "record_set_len "
      "{}",
      p.id,
      p.error,
      p.high_watermark,
      p.last_stable_offset,
      p.aborted_transactions,
      p.record_set.size_bytes());
}

std::ostream& operator<<(std::ostream& o, const fetch_response::partition& p) {
    return fmt_print(o, "name {} responses {}", p.name, p.responses);
}

std::ostream& operator<<(std::ostream& o, const fetch_response& r) {
    return fmt_print(o, "partitions {}", r.partitions);
}

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
                    wr.write(error_code::none);
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
} // namespace kafka
