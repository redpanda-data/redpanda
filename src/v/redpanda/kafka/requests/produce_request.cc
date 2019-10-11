#include "redpanda/kafka/requests/produce_request.h"

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "redpanda/kafka/default_namespace.h"
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/kafka_batch_adapter.h"
#include "storage/shard_assignment.h"
#include "utils/fragbuf.h"
#include "utils/remote.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

struct partition_result {
    model::partition_id partition;
    errors::error_code error;
    model::offset base_offset;
    model::timestamp log_append_time;
    int64_t log_start_offset;
};

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
                rw.write(int64_t(pr.base_offset.value()));
                rw.write(pr.log_append_time.value());
                rw.write(pr.log_start_offset);
            });
      });
    resp->writer().write(int32_t(0));

    resp->writer().write(throttle);
    return make_foreign(std::move(resp));
}

future<partition_result> do_process(
  remote<request_context>& ctx,
  std::unique_ptr<model::ntp> ntp,
  fragbuf batch) {
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
  fragbuf>;

static thread_local execution_stage_type produce_stage{"produce", &do_process};

future<response_ptr>
produce_request::process(request_context&& ctx, smp_service_group g) {
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

} // namespace kafka::requests
