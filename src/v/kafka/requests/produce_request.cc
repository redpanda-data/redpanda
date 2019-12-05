#include "kafka/requests/produce_request.h"

#include "bytes/iobuf.h"
#include "kafka/default_namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "storage/shard_assignment.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

struct partition_result {
    model::partition_id partition;
    error_code error;
    model::offset base_offset;
    model::timestamp log_append_time;
    int64_t log_start_offset;
};
void produce_request::encode(
  const request_context& ctx, response_writer& writer) {
    auto version = ctx.header().version;

    writer.write(transactional_id);
    writer.write(int16_t(acks));
    writer.write(int32_t(timeout.count()));
    writer.write_array(topics, [](topic& t, response_writer& writer) {
        writer.write(t.name);
        writer.write_array(t.data, [](topic_data& td, response_writer& writer) {
            writer.write(td.id());
            writer.write(std::move(td.data));
        });
    });
}

void produce_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    transactional_id = reader.read_nullable_string();
    acks = reader.read_int16();
    timeout = std::chrono::milliseconds(reader.read_int32());
    topics = reader.read_array([](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .data = reader.read_array([](request_reader& reader) {
              return topic_data{
                .id = model::partition_id(reader.read_int32()),
                .data = reader.read_fragmented_nullable_bytes(),
              };
          }),
        };
    });
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::topic_data& d) {
    return fmt_print(o, "id {} payload {}", d.id, d.data);
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::topic& t) {
    return fmt_print(o, "name {} data {}", t.name, t.data);
}

std::ostream& operator<<(std::ostream& o, const produce_request& r) {
    return fmt_print(
      o,
      "txn_id {} acks {} timeout {} topics {}",
      r.transactional_id,
      r.acks,
      r.timeout,
      r.topics);
}

void produce_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    writer.write_array(topics, [version](topic& t, response_writer& writer) {
        writer.write(t.name);
        writer.write_array(
          t.partitions, [version](partition& p, response_writer& writer) {
              writer.write(p.id);
              writer.write(p.error);
              writer.write(int64_t(p.base_offset()));
              writer.write(p.log_append_time.value());
              if (version >= api_version(5)) {
                  writer.write(int64_t(p.log_start_offset()));
              }
          });
    });
    writer.write(int32_t(throttle.count()));
}

struct topic_result {
    std::string_view topic;
    std::vector<partition_result> results;
};

response_ptr
write_replies(std::vector<topic_result> topic_results, int32_t throttle) {
    auto resp = std::make_unique<response>();
    resp->writer().write_array(
      topic_results, [](const topic_result& tr, response_writer& rw) {
          rw.write(tr.topic);
          rw.write_array(
            tr.results, [](const partition_result& pr, response_writer& rw) {
                rw.write(pr.partition);
                rw.write(pr.error);
                rw.write(int64_t(pr.base_offset()));
                rw.write(pr.log_append_time.value());
                rw.write(pr.log_start_offset);
            });
      });
    resp->writer().write(int32_t(0));

    resp->writer().write(throttle);
    return make_foreign(std::move(resp));
}

future<partition_result> do_process(
  remote<request_context>& ctx, std::unique_ptr<model::ntp> ntp, iobuf batch) {
    auto reader = reader_from_kafka_batch(std::move(batch));
    // TODO: Call into consensus
    (void)reader;
    return make_exception_future<partition_result>(
      std::runtime_error("Unsupported"));
}

using execution_stage_type = inheriting_concrete_execution_stage<
  future<partition_result>,
  remote<request_context>&,
  std::unique_ptr<model::ntp>,
  iobuf>;

static thread_local execution_stage_type produce_stage{"produce", &do_process};

future<response_ptr>
produce_api::process(request_context&& ctx, smp_service_group g) {
    return do_with(
      remote(std::move(ctx)), [g](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          ctx.reader().skip_nullable_string(); // Skip the transactional ID.
          auto acks = ctx.reader().read_int16();
          auto timeout = ctx.reader().read_int32();
          auto fs = ctx.reader().read_array([&](request_reader& rr) {
              auto topic = rr.read_string_view();
              auto partition_results = rr.read_array(
                [&](request_reader& rr) mutable {
                    // FIXME: Introduce ntp_view,
                    // which should be noexcept moveable
                    auto ntp = model::ntp{
                      default_namespace(),
                      model::topic_partition{
                        model::topic(sstring(topic.data(), topic.size())),
                        model::partition_id(rr.read_int32())}};
                    auto batch = rr.read_fragmented_nullable_bytes();
                    if (!batch) {
                        throw std::runtime_error(
                          "Produce requests must have a record batch");
                    }
                    // FIXME: Legacy shard assignment
                    auto shard = storage::shard_of(ntp);
                    return smp::submit_to(
                      shard,
                      g,
                      [&remote_ctx,
                       ntp = std::move(ntp),
                       batch = std::move(*batch)]() mutable {
                          return produce_stage(
                            seastar::ref(remote_ctx),
                            std::make_unique<model::ntp>(std::move(ntp)),
                            std::move(batch));
                      });
                });
              return when_all_succeed(
                       partition_results.begin(), partition_results.end())
                .then([topic](std::vector<partition_result>&& resp) mutable {
                    return topic_result{std::move(topic), std::move(resp)};
                });
          });
          return when_all_succeed(fs.begin(), fs.end())
            .then([throttle = ctx.throttle_delay_ms()](
                    std::vector<topic_result> topic_results) {
                return write_replies(std::move(topic_results), throttle);
            });
      });
}

} // namespace kafka
