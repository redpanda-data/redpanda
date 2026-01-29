// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
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

static cloud_topics::cluster_epoch min_epoch{3840};

using namespace std::chrono_literals;

namespace cloud_topics {

// Sink consumes and acknowledges all write requests
// in the pipeline.
struct pipeline_sink {
    explicit pipeline_sink(l0::write_pipeline<>& p)
      : stage(p.register_write_pipeline_stage()) {
        vlog(test_log.info, "pipeline_sink stage: {}", stage.id());
    }

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
            vlog(
              test_log.debug,
              "pipeline_sink subscribe, stage id {}",
              stage.id());
            auto res = co_await stage.wait_next(&_as);
            vlog(test_log.debug, "pipeline_sink event");
            if (!res.has_value()) {
                vlog(
                  test_log.error, "Event subscription failed: {}", res.error());
                continue;
            }
            auto event = res.value();
            if (event.type != l0::event_type::new_write_request) {
                co_return;
            }
            // Vacuum all write requests
            auto result = stage.pull_write_requests(
              std::numeric_limits<size_t>::max());
            for (auto& r : result.requests) {
                if (error_generator) {
                    auto errc = error_generator(r);
                    if (errc != errc::success) {
                        r.set_value(errc);
                        write_requests_acked++;
                        vlog(
                          test_log.debug,
                          "Write request NACK({}), total ack: {}",
                          errc,
                          write_requests_acked);
                        continue;
                    }
                }
                r.set_value(chunked_vector<extent_meta>{});
                write_requests_acked++;
                vlog(
                  test_log.debug,
                  "Write request ACK, total ack: {}",
                  write_requests_acked);
            }
        }
    }

    l0::write_pipeline<>::stage stage;
    ss::gate _gate;
    ss::abort_source _as;
    size_t write_requests_acked{0};
    // This function is used to simulate upload failures
    std::function<errc(l0::write_request<>&)> error_generator;
};

namespace l0 {
struct write_request_balancer_accessor {
    static void disable_data_threshold_uploads(write_request_scheduler<>* s) {
        s->_test_only_disable_data_threshold = true;
    }
    static void disable_time_based_fallback(write_request_scheduler<>* s) {
        s->_test_only_disable_time_based_fallback = true;
    }
};
} // namespace l0

} // namespace cloud_topics

using namespace cloud_topics;

class write_request_balancer_fixture : public seastar_test {
public:
    ss::future<>
    start(bool disable_data_threshold, bool disable_time_based_fallback) {
        vlog(test_log.info, "Creating pipeline");
        co_await pipeline.start();

        // Start the scheduler
        vlog(test_log.info, "Creating scheduler");
        co_await scheduler.start(ss::sharded_parameter([this] {
            return pipeline.local().register_write_pipeline_stage();
        }));

        co_await scheduler.invoke_on_all(
          [disable_data_threshold,
           disable_time_based_fallback](l0::write_request_scheduler<>& s) {
              if (disable_data_threshold) {
                  l0::write_request_balancer_accessor::
                    disable_data_threshold_uploads(&s);
              }
              if (disable_time_based_fallback) {
                  l0::write_request_balancer_accessor::
                    disable_time_based_fallback(&s);
              }
          });

        vlog(test_log.info, "Starting scheduler");
        co_await scheduler.invoke_on_all(
          [](l0::write_request_scheduler<>& sched) { return sched.start(); });

        // Start the sink
        vlog(test_log.info, "Creating request_sink");
        co_await request_sink.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));
        vlog(test_log.info, "Starting request_sink");
        co_await request_sink.invoke_on_all(
          [](pipeline_sink& sink) { return sink.start(); });
    }

    ss::future<> stop() {
        vlog(test_log.info, "Stopping request_sink");
        co_await request_sink.stop();
        vlog(test_log.info, "Stopping scheduler");
        co_await scheduler.stop();
        vlog(test_log.info, "Stopping pipeline");
        co_await pipeline.stop();
    }

    ss::sharded<l0::write_pipeline<>> pipeline;
    ss::sharded<l0::write_request_scheduler<>> scheduler;
    ss::sharded<pipeline_sink> request_sink;
};

static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));

static const model::ntp
  test_ntp0(test_topic.ns, test_topic.tp, model::partition_id(0));

static const model::ntp
  test_ntp1(test_topic.ns, test_topic.tp, model::partition_id(1));

/// Returns expected number of requests
static ss::future<size_t> write_until_threshold(
  write_request_balancer_fixture& fix, size_t size_threshold) {
    size_t num_requests_sent = 0;
    size_t total_size = 0;
    std::vector<
      ss::future<std::expected<chunked_vector<extent_meta>, std::error_code>>>
      wd;
    while (total_size < size_threshold) {
        auto buf = co_await model::test::make_random_batches();
        chunked_vector<model::record_batch> batches;
        for (auto& b : buf) {
            total_size += b.size_bytes();
            batches.push_back(std::move(b));
        }

        num_requests_sent++;
        wd.push_back(fix.pipeline.local().write_and_debounce(
          test_ntp0,
          min_epoch,
          std::move(batches),
          ss::lowres_clock::now() + 10s));

        vlog(
          test_log.info,
          "Produced {} requests and {} bytes",
          num_requests_sent,
          total_size);
    }
    auto results = co_await ss::when_all(wd.begin(), wd.end());
    // no errors expected
    bool failure = false;
    for (auto& r : results) {
        if (r.failed()) {
            vlog(
              test_log.error,
              "Unexpected write failure: {}",
              r.get_exception());
            failure = true;
        }
    }
    if (failure) {
        throw std::runtime_error("Unexpected failure");
    }
    co_return num_requests_sent;
}

/// Same as model::test_make_random_batches but allows to specify number of
/// batches and size of every batch (same size for all)
static auto make_random_batches(size_t num_batches, size_t batch_size) {
    chunked_vector<model::record_batch> batches;
    for (size_t i = 0; i < num_batches; i++) {
        auto batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset(i),
            .allow_compression = false,
            .count = 1,
            .records = 1,
            .record_sizes = std::vector<size_t>{batch_size},
          });
        batches.push_back(std::move(batch));
    }
    return batches;
}

TEST_F_CORO(write_request_balancer_fixture, time_based_fallback_test) {
    // This test produces some batches and expects time based threshold
    // mechanism to trigger the upload.
    ASSERT_TRUE_CORO(ss::smp::count > 1);

    co_await start(
      /*disable_data_threshold=*/true, /*disable_time_based_fallback=*/false);

    auto buf = co_await model::test::make_random_batches();
    chunked_vector<model::record_batch> batches;
    for (auto& b : buf) {
        batches.push_back(std::move(b));
    }

    auto placeholders = co_await pipeline.local().write_and_debounce(
      test_ntp0, min_epoch, std::move(batches), ss::lowres_clock::now() + 10s);

    // Check that number of upload requests matches expectation.
    // The current shard should receive the write request.
    ASSERT_EQ_CORO(request_sink.local().write_requests_acked, 1);

    co_await stop();
}

TEST_F_CORO(write_request_balancer_fixture, test_core_affinity) {
    // This test generates batches on two different shards and expects
    // time-based-fallback mechanism to trigger the upload. It expects
    // that write requests to land on one shard. The shard that receives
    // requests is expected to be the one that produced the most data.
    ASSERT_TRUE_CORO(ss::smp::count > 1);

    co_await scheduler.invoke_on_all([](l0::write_request_scheduler<>& s) {
        l0::write_request_balancer_accessor::disable_data_threshold_uploads(&s);
    });

    co_await start(
      /*disable_data_threshold=*/true, /*disable_time_based_fallback=*/false);

    static constexpr size_t batch_size = 0x1000;
    static constexpr size_t num_batches_hi = 99;
    static constexpr size_t num_batches_lo = 10;

    std::atomic<size_t> shard1_data_size = 0;
    auto fut = ss::smp::submit_to(ss::shard_id(1), [this, &shard1_data_size] {
        auto batches = make_random_batches(num_batches_hi, batch_size);
        for (const model::record_batch& b : batches) {
            shard1_data_size += b.size_bytes();
        }
        vlog(test_log.info, "Calling write_and_debounce on shard 1");
        return pipeline.local().write_and_debounce(
          test_ntp0,
          min_epoch,
          std::move(batches),
          ss::lowres_clock::now() + 10s);
    });

    auto batches = make_random_batches(num_batches_lo, batch_size);
    vlog(test_log.info, "Calling write_and_debounce on shard 0");
    auto s0_placeholders = co_await pipeline.local().write_and_debounce(
      test_ntp0, min_epoch, std::move(batches), ss::lowres_clock::now() + 10s);

    auto s1_placeholders = co_await std::move(fut);

    // Check that number of upload requests matches expectation.
    // The shard that has the most data should handle the request.
    auto num_requests = co_await request_sink.invoke_on(
      ss::shard_id(1), [](auto& s) { return s.write_requests_acked; });
    ASSERT_EQ_CORO(num_requests, 2);

    co_await stop();
}

TEST_F_CORO(write_request_balancer_fixture, data_threshold_test) {
    ASSERT_TRUE_CORO(ss::smp::count > 1);
    auto size_threshold
      = config::shard_local_cfg()
          .cloud_topics_produce_batching_size_threshold.value();

    co_await start(
      /*disable_data_threshold=*/false, /*disable_time_based_fallback=*/true);

    auto num_requests_sent = co_await write_until_threshold(
      *this, size_threshold);

    // Check that number of upload requests matches expectation.
    ASSERT_EQ_CORO(
      request_sink.local().write_requests_acked, num_requests_sent);

    co_await stop();
}

TEST_F_CORO(write_request_balancer_fixture, test_data_threshold_with_failover) {
    // On shard zero produce little data on shard 0 and in parallel produce a
    // lot of data on shard 1. Expect data threshold to be reached on shard 1
    // and time based threshold to be reached on shard 0.

    ASSERT_TRUE_CORO(ss::smp::count > 1);
    auto size_threshold
      = config::shard_local_cfg()
          .cloud_topics_produce_batching_size_threshold.value();

    co_await start(false, false);

    // Produce on shard 0
    auto buf = co_await model::test::make_random_batches();
    chunked_vector<model::record_batch> batches;
    for (auto& b : buf) {
        batches.push_back(std::move(b));
    }
    auto placeholders = co_await pipeline.local().write_and_debounce(
      test_ntp0, min_epoch, std::move(batches), ss::lowres_clock::now() + 10s);

    // Produce on shard 1
    auto expected_num_requests1 = co_await ss::smp::submit_to(
      1, [this, size_threshold] {
          return write_until_threshold(*this, size_threshold);
      });

    ASSERT_EQ_CORO(request_sink.local().write_requests_acked, 1);
    auto actual_num_requests1 = co_await request_sink.invoke_on(
      1, [](auto& s) { return s.write_requests_acked; });
    ASSERT_EQ_CORO(expected_num_requests1, actual_num_requests1);

    co_await stop();
}

TEST_F_CORO(
  write_request_balancer_fixture, test_time_based_fallback_with_failures) {
    // This test checks different combinations of failures.
    ASSERT_TRUE_CORO(ss::smp::count > 1);
    static constexpr size_t batch_size = 0x1000;

    co_await scheduler.invoke_on_all([](l0::write_request_scheduler<>& s) {
        l0::write_request_balancer_accessor::disable_data_threshold_uploads(&s);
    });

    co_await start(
      /*disable_data_threshold=*/true, /*disable_time_based_fallback=*/false);

    co_await request_sink.invoke_on_all([](pipeline_sink& s) {
        // Inject failure on test_ntp1
        s.error_generator = [](l0::write_request<>& r) {
            if (r.ntp == test_ntp1) {
                return cloud_topics::errc::upload_failure;
            }
            return cloud_topics::errc::success;
        };
    });

    auto write_on_shard = [&](ss::shard_id shard, model::ntp ntp, size_t size) {
        return pipeline.invoke_on(
          shard, [ntp, size](l0::write_pipeline<>& pipeline) {
              // Produce small batch on shard 1 so shard 0 will be
              // uploading the data.
              chunked_vector<model::record_batch> batches;
              auto batch = model::test::make_random_batch(
                model::test::record_batch_spec{
                  .offset = model::offset(0),
                  .allow_compression = false,
                  .count = 1,
                  .records = 1,
                  .record_sizes = std::vector<size_t>{size},
                });
              batches.push_back(std::move(batch));
              vlog(test_log.info, "Calling write_and_debounce");
              // The failure is injected based on ntp
              return pipeline.write_and_debounce(
                ntp,
                min_epoch,
                std::move(batches),
                ss::lowres_clock::now() + 10s);
          });
    };

    // This case generates batches on two different shards and expects
    // time-based-fallback mechanism to trigger the upload. The test
    // case injects failure on shard 1 and expects the fallback mechanism
    // to propagate them back to the caller. The target shard in this case
    // is shard 0 (the shard that makes the upload).
    {
        // Produce on shard 1
        auto fut11 = write_on_shard(ss::shard_id(1), test_ntp1, batch_size);
        auto fut10 = write_on_shard(ss::shard_id(1), test_ntp0, batch_size);

        // Produce on shard 0
        auto fut00 = write_on_shard(ss::shard_id(0), test_ntp0, batch_size * 4);

        auto ph00 = co_await std::move(fut00);
        auto ph10 = co_await std::move(fut10);
        auto ph11 = co_await std::move(fut11);

        // Check that number of upload requests matches expectation.
        // The shard that has the most data should handle the request.
        auto num_requests = request_sink.local().write_requests_acked;
        ASSERT_EQ_CORO(num_requests, 3);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph11.has_value());
        ASSERT_TRUE_CORO(ph11.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }
    // This test case is about the same as the previous one with the only
    // difference that the failure is injected on shard 0 (the uploading shard).
    {
        // Produce on shard 1
        auto fut10 = write_on_shard(ss::shard_id(1), test_ntp0, batch_size);

        // Produce on shard 0
        auto fut01 = write_on_shard(
          ss::shard_id(0), test_ntp1, batch_size * 2); // will fail
        auto fut00 = write_on_shard(ss::shard_id(0), test_ntp0, batch_size * 2);

        auto ph00 = co_await std::move(fut00);
        auto ph01 = co_await std::move(fut01);
        auto ph10 = co_await std::move(fut10);

        auto num_requests = request_sink.local().write_requests_acked;
        ASSERT_EQ_CORO(num_requests, 6);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph01.has_value());
        ASSERT_TRUE_CORO(ph01.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }
    // The data is uploaded on shard 1 and the failure is injected on shard 0.
    {
        // Produce on shard 0
        auto fut00 = write_on_shard(ss::shard_id(0), test_ntp0, batch_size);
        auto fut01 = write_on_shard(
          ss::shard_id(0), test_ntp1, batch_size); // will fail
        // Produce on shard 1
        auto fut10 = write_on_shard(ss::shard_id(1), test_ntp0, batch_size * 4);

        auto ph00 = co_await std::move(fut00);
        auto ph01 = co_await std::move(fut01);
        auto ph10 = co_await std::move(fut10);

        auto num_requests0 = request_sink.local().write_requests_acked;
        // Nothing is uploaded on this shard
        ASSERT_EQ_CORO(num_requests0, 6);
        auto num_requests1 = co_await request_sink.invoke_on(
          ss::shard_id(1), [](auto& s) { return s.write_requests_acked; });
        ASSERT_EQ_CORO(num_requests1, 3);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph01.has_value());
        ASSERT_TRUE_CORO(ph01.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }

    co_await stop();
}
