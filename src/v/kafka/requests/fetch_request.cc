#include "kafka/requests/fetch_request.h"

#include "kafka/errors.h"
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

future<response_ptr>
fetch_api::process(request_context&& ctx, smp_service_group g) {
    return do_with(std::move(ctx), [g](request_context& ctx) {
        fetch_request request;
        request.decode(ctx);

        std::vector<future<fetch_response::partition>> partitions;
        for (auto& t : request.topics) {
            std::vector<future<fetch_response::partition_response>> responses;
            for (auto& p : t.partitions) {
                fetch_response::partition_response r;
                r.id = p.id;
                r.error = error_code::none;
                r.high_watermark = model::offset(0);
                r.last_stable_offset = model::offset(0);
                auto batches = storage::test::make_random_batches(
                  p.fetch_offset, 5);
                auto reader = model::make_memory_record_batch_reader(
                  std::move(batches));
                auto f = do_with(
                  std::move(reader),
                  [r = std::move(r)](
                    model::record_batch_reader& reader) mutable {
                      return reader
                        .consume(kafka_batch_serializer(), model::no_timeout)
                        .then([r = std::move(r)](iobuf&& batch) mutable {
                            r.record_set = std::move(batch);
                            return std::move(r);
                        });
                  });
                responses.push_back(std::move(f));
            }
            auto f = when_all_succeed(responses.begin(), responses.end())
                       .then([name = std::move(t.name)](
                               std::vector<fetch_response::partition_response>
                                 responses) mutable {
                           return fetch_response::partition{
                             .name = std::move(name),
                             .responses = std::move(responses)};
                       });
            partitions.push_back(std::move(f));
        }

        return when_all_succeed(partitions.begin(), partitions.end())
          .then([&ctx](std::vector<fetch_response::partition>&& partitions) {
              fetch_response reply;
              reply.partitions = std::move(partitions);
              auto resp = std::make_unique<response>();
              reply.encode(ctx, *resp.get());
              return make_ready_future<response_ptr>(std::move(resp));
          });
    });
}

} // namespace kafka
