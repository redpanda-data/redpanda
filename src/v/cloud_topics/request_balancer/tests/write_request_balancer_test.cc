// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/request_balancer/write_request_balancer.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <limits>

inline ss::logger test_log("balancer_gtest");

namespace cloud_topics = experimental::cloud_topics;
using namespace std::chrono_literals;

namespace experimental::cloud_topics {

// Sink consumes and acknowledges all write requests
// in the pipeline.
struct pipeline_sink {
    explicit pipeline_sink(core::write_pipeline<>& p)
      : pipeline(p)
      , _id(pipeline.register_pipeline_stage()) {}

    ss::future<> start() {
        ssx::background = bg_run();
        co_return;
    }

    ss::future<> stop() {
        _as.request_abort();
        co_await _gate.close();
    }

    ss::future<> bg_run() {
        auto h = _gate.hold();
        while (!_as.abort_requested()) {
            vlog(test_log.debug, "pipeline_sink subscribe, stage id {}", _id);
            core::event_filter<> flt(core::event_type::new_write_request, _id);
            auto sub = _as.subscribe(
              [&flt](const std::optional<std::exception_ptr>&) noexcept {
                  flt.cancel();
              });
            auto fut = co_await ss::coroutine::as_future(
              pipeline.subscribe(flt));
            vlog(test_log.debug, "pipeline_sink event");
            if (fut.failed()) {
                vlog(
                  test_log.error,
                  "Event subscription failed: {}",
                  fut.get_exception());
                continue;
            }
            auto event = fut.get();
            if (event.type != core::event_type::new_write_request) {
                co_return;
            }
            // Vacuum all write requests
            auto result = pipeline.get_write_requests(
              std::numeric_limits<size_t>::max(), _id);
            for (auto& r : result.ready) {
                // Set empty result to unblock the caller
                r.set_value(
                  ss::circular_buffer<
                    model::record_batch>{}); // TODO: return some random batch
                write_requests_acked++;
            }
        }
    }

    core::write_pipeline<>& pipeline;
    core::pipeline_stage _id;
    ss::gate _gate;
    ss::abort_source _as;
    size_t write_requests_acked{0};
};

/// Balancing policy that redirects all write requests
/// to shard 1
class shard_one_balancing_policy : public balancing_policy {
public:
    void rebalance(std::vector<shard_resource_utilization>& shards) override {
        shards.front().shard = 1;
    }
};

} // namespace experimental::cloud_topics

class write_request_balancer_fixture : public seastar_test {
public:
    ss::future<> start() {
        vlog(test_log.info, "Creating pipeline");
        co_await pipeline.start();

        // Start the balancer
        vlog(test_log.info, "Creating balancer");
        co_await balancer.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }),
          ss::sharded_parameter([] {
              return std::make_unique<
                cloud_topics::shard_one_balancing_policy>();
          }));
        vlog(test_log.info, "Starting balancer");
        co_await balancer.invoke_on_all(
          [](cloud_topics::write_request_balancer& bal) {
              return bal.start();
          });

        // Start the sink
        vlog(test_log.info, "Creating request_sink");
        co_await request_sink.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));
        vlog(test_log.info, "Starting request_sink");
        co_await request_sink.invoke_on_all(
          [](cloud_topics::pipeline_sink& sink) { return sink.start(); });
    }

    ss::future<> stop() {
        vlog(test_log.info, "Stopping request_sink");
        co_await request_sink.stop();
        vlog(test_log.info, "Stopping balancer");
        co_await balancer.stop();
        vlog(test_log.info, "Stopping pipeline");
        co_await pipeline.stop();
    }

    ss::sharded<cloud_topics::core::write_pipeline<>> pipeline;
    ss::sharded<cloud_topics::write_request_balancer> balancer;
    ss::sharded<cloud_topics::pipeline_sink> request_sink;
};

static const model::ntp test_ntp(
  model::kafka_namespace,
  model::topic_partition(model::topic("tapioca"), model::partition_id(42)));

TEST_F_CORO(write_request_balancer_fixture, smoke_test) {
    ASSERT_TRUE_CORO(ss::smp::count > 1);

    co_await start();

    auto buf = co_await model::test::make_random_batches();
    auto reader = model::make_memory_record_batch_reader(std::move(buf));

    auto placeholders = co_await pipeline.local().write_and_debounce(
      test_ntp, std::move(reader), 1s);

    co_await stop();
}
