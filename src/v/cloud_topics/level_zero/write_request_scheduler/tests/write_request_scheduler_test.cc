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
#include <map>
#include <random>
#include <set>

inline ss::logger test_log("balancer_gtest");

static cloud_topics::cluster_epoch min_epoch{3840};

using namespace std::chrono_literals;

namespace cloud_topics {

// Sink consumes and acknowledges all write requests
// in the pipeline.
struct pipeline_sink {
    explicit pipeline_sink(l0::write_pipeline<>::stage s)
      : stage(s) {
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
                r.set_value(upload_meta{});
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
    static void disable_background_loop(write_request_scheduler<>* s) {
        s->_test_only_disable_background_loop = true;
    }
};
} // namespace l0

} // namespace cloud_topics

using namespace cloud_topics;

class write_request_balancer_fixture : public seastar_test {
public:
    ss::future<> start(bool disable_background_loop) {
        vlog(test_log.info, "Creating pipeline");
        co_await pipeline.start();

        // Create the scheduler (registers stage 0 on each shard)
        vlog(test_log.info, "Creating scheduler");
        co_await scheduler.start(ss::sharded_parameter([this] {
            return pipeline.local().register_write_pipeline_stage();
        }));

        co_await scheduler.invoke_on_all(
          [disable_background_loop](l0::write_request_scheduler<>& s) {
              if (disable_background_loop) {
                  l0::write_request_balancer_accessor::disable_background_loop(
                    &s);
              }
          });

        // Start scheduler - can be called before sink is created since
        // next_stage() now returns unassigned_pipeline_stage when next
        // stage is not registered
        vlog(test_log.info, "Starting scheduler");
        co_await scheduler.invoke_on_all(
          [](l0::write_request_scheduler<>& sched) { return sched.start(); });

        // Create and start the sink
        vlog(test_log.info, "Creating request_sink");
        co_await request_sink.start(ss::sharded_parameter([this] {
            return pipeline.local().register_write_pipeline_stage();
        }));
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
    std::vector<ss::future<std::expected<upload_meta, std::error_code>>> wd;
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
            auto ex = r.get_exception();
            vlog(test_log.error, "Unexpected write failure: {}", ex);
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

TEST_F_CORO(write_request_balancer_fixture, time_deadline_test) {
    // This test produces some batches and expects the time deadline
    // to trigger the upload.
    ASSERT_TRUE_CORO(ss::smp::count > 1);

    co_await start(false);

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
    // the unified scheduling policy to trigger the upload. It expects
    // that write requests to land on one shard using round-robin scheduling.
    // With round-robin, shard 0 will handle the first upload (ix=0).
    ASSERT_TRUE_CORO(ss::smp::count > 1);

    co_await start(false);

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
    // With round-robin scheduling, shard 0 handles the first upload (ix=0).
    // Both write requests from shard 0 and shard 1 are forwarded to shard 0.
    auto num_requests = co_await request_sink.invoke_on(
      ss::shard_id(0), [](auto& s) { return s.write_requests_acked; });
    ASSERT_EQ_CORO(num_requests, 2);

    co_await stop();
}

TEST_F_CORO(write_request_balancer_fixture, data_threshold_test) {
    // Single shard produces enough data to trigger L0 upload
    ASSERT_TRUE_CORO(ss::smp::count > 1);
    auto size_threshold
      = config::shard_local_cfg()
          .cloud_topics_produce_batching_size_threshold.value();

    co_await start(false);

    auto num_requests_sent = co_await write_until_threshold(
      *this, size_threshold);

    // With round-robin scheduling, requests can be handled by either shard.
    // Verify total requests across all shards equals expected.
    auto total_requests_shard0 = request_sink.local().write_requests_acked;
    auto total_requests_shard1 = co_await request_sink.invoke_on(
      1, [](auto& s) { return s.write_requests_acked; });
    ASSERT_EQ_CORO(
      total_requests_shard0 + total_requests_shard1, num_requests_sent);

    co_await stop();
}

TEST_F_CORO(write_request_balancer_fixture, test_data_threshold_with_failover) {
    // On shard zero produce little data on shard 0 and in parallel produce a
    // lot of data on shard 1. With round-robin scheduling, the target shard
    // alternates between shards. This test verifies that all requests are
    // eventually acknowledged regardless of which shard handles the upload.

    ASSERT_TRUE_CORO(ss::smp::count > 1);
    auto size_threshold
      = config::shard_local_cfg()
          .cloud_topics_produce_batching_size_threshold.value();

    co_await start(false);

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

    // With round-robin, requests can be handled by either shard.
    // Verify total requests across all shards equals expected.
    auto total_requests_shard0 = request_sink.local().write_requests_acked;
    auto total_requests_shard1 = co_await request_sink.invoke_on(
      1, [](auto& s) { return s.write_requests_acked; });
    ASSERT_EQ_CORO(
      total_requests_shard0 + total_requests_shard1,
      1 + expected_num_requests1);

    co_await stop();
}

TEST_F_CORO(
  write_request_balancer_fixture, test_cross_shard_upload_with_failures) {
    // This test checks different combinations of failures during cross-shard
    // uploads. With round-robin scheduling, the target shard is selected based
    // on the ix counter, not on data volume. The test verifies that failures
    // are correctly propagated regardless of which shard handles the upload.
    ASSERT_TRUE_CORO(ss::smp::count > 1);
    static constexpr size_t batch_size = 0x1000;

    co_await start(false);

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

    auto get_total_acked = [&]() {
        auto shard0 = request_sink.local().write_requests_acked;
        return request_sink.invoke_on(ss::shard_id(1), [shard0](auto& s) {
            return shard0 + s.write_requests_acked;
        });
    };

    // This case generates batches on two different shards and expects
    // the unified scheduling policy to trigger the upload. The test
    // case injects failure on test_ntp1 and expects the scheduler
    // to propagate them back to the caller.
    {
        // Produce on shard 1
        auto fut11 = write_on_shard(ss::shard_id(1), test_ntp1, batch_size);
        auto fut10 = write_on_shard(ss::shard_id(1), test_ntp0, batch_size);

        // Produce on shard 0
        auto fut00 = write_on_shard(ss::shard_id(0), test_ntp0, batch_size * 4);

        auto ph00 = co_await std::move(fut00);
        auto ph10 = co_await std::move(fut10);
        auto ph11 = co_await std::move(fut11);

        // Check that total upload requests matches expectation.
        auto total_requests = co_await get_total_acked();
        ASSERT_EQ_CORO(total_requests, 3);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph11.has_value());
        ASSERT_TRUE_CORO(ph11.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }
    // This test case is about the same as the previous one with different
    // data distribution.
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

        auto total_requests = co_await get_total_acked();
        ASSERT_EQ_CORO(total_requests, 6);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph01.has_value());
        ASSERT_TRUE_CORO(ph01.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }
    // Another case with different data distribution.
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

        auto total_requests = co_await get_total_acked();
        ASSERT_EQ_CORO(total_requests, 9);

        ASSERT_TRUE_CORO(ph00.has_value());
        ASSERT_TRUE_CORO(!ph01.has_value());
        ASSERT_TRUE_CORO(ph01.error() == cloud_topics::errc::upload_failure);
        ASSERT_TRUE_CORO(ph10.has_value());
    }

    co_await stop();
}

// ============================================================================
// scheduler_context unit tests
// ============================================================================

// Helper to create a scheduler_context for testing with mock atomic counters
class scheduler_context_test : public testing::Test {
public:
    using clock_type = ss::manual_clock;

    // Mock backlog counters - one per shard
    // Using unique_ptr because std::atomic is not movable
    std::vector<std::unique_ptr<std::atomic<size_t>>> backlog_counters;
    std::vector<std::unique_ptr<std::atomic<size_t>>> next_stage_counters;

    std::unique_ptr<l0::scheduler_context<clock_type>>
    make_context(size_t num_shards) {
        backlog_counters.clear();
        next_stage_counters.clear();

        for (size_t i = 0; i < num_shards; i++) {
            backlog_counters.push_back(
              std::make_unique<std::atomic<size_t>>(0));
            next_stage_counters.push_back(
              std::make_unique<std::atomic<size_t>>(0));
        }

        // FixedArrays are sized at construction time
        auto ctx = std::make_unique<l0::scheduler_context<clock_type>>(
          num_shards);

        // Initialize shards with references to our mock counters.
        // shard_to_group and groups are already default-initialized.
        for (size_t i = 0; i < num_shards; i++) {
            ctx->shards[i] = l0::shard_state<clock_type>(
              std::ref(*backlog_counters[i]),
              std::ref(
                static_cast<const std::atomic<size_t>&>(
                  *next_stage_counters[i])));
        }

        return ctx;
    }

    void set_backlog(size_t shard, size_t bytes) {
        backlog_counters[shard]->store(bytes);
    }

    void set_next_stage_backlog(size_t shard, size_t bytes) {
        next_stage_counters[shard]->store(bytes);
    }

    l0::write_request_scheduler_probe probe{true};
};

TEST_F(scheduler_context_test, test_initial_state) {
    // Test that all shards start in group 0
    auto ctx = make_context(4);

    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 4);
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{0});
}

TEST_F(scheduler_context_test, test_single_split) {
    // Test splitting a 4-shard group into two 2-shard groups
    auto ctx = make_context(4);

    // Split group 0
    bool split = ctx->try_split_group(l0::group_id{0});
    EXPECT_TRUE(split);

    // After split: shards 0,1 in group 0, shards 2,3 in group 2
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{2});

    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 2);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{2}), 2);
}

TEST_F(scheduler_context_test, test_full_split_buddy_algorithm) {
    // Test the full buddy allocator split pattern:
    // [0,1,2,3] -> [0,1] [2,3] -> [0] [1] [2,3] -> [0] [1] [2] [3]
    auto ctx = make_context(4);

    // Initial state: all in group 0
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 4);

    // First split: [0,1,2,3] -> [0,1] + [2,3]
    EXPECT_TRUE(ctx->try_split_group(l0::group_id{0}));
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{2});

    // Second split: [0,1] -> [0] + [1]
    EXPECT_TRUE(ctx->try_split_group(l0::group_id{0}));
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{1});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{2});

    // Third split: [2,3] -> [2] + [3]
    EXPECT_TRUE(ctx->try_split_group(l0::group_id{2}));
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{1});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{3});

    // Each shard is now in its own group
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{1}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{2}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{3}), 1);

    // Cannot split single-shard groups
    EXPECT_FALSE(ctx->try_split_group(l0::group_id{0}));
    EXPECT_FALSE(ctx->try_split_group(l0::group_id{1}));
}

TEST_F(scheduler_context_test, test_find_buddy_group) {
    auto ctx = make_context(4);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});

    // Group 0's buddy is group 2
    auto buddy0 = ctx->find_buddy_group(l0::group_id{0});
    ASSERT_TRUE(buddy0.has_value());
    EXPECT_EQ(buddy0.value(), l0::group_id{2});

    // Group 2 has no buddy (it's the last group)
    auto buddy2 = ctx->find_buddy_group(l0::group_id{2});
    EXPECT_FALSE(buddy2.has_value());
}

TEST_F(scheduler_context_test, test_merge_buddy_groups) {
    auto ctx = make_context(4);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 2);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{2}), 2);

    // Merge group 0 with its buddy (group 2)
    bool merged = ctx->try_merge_group(l0::group_id{0});
    EXPECT_TRUE(merged);

    // All shards should be back in group 0
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{0});
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 4);
}

TEST_F(scheduler_context_test, test_full_merge_buddy_algorithm) {
    // Test the full buddy allocator merge pattern (reverse of split):
    // [0] [1] [2] [3] -> [0] [1] [2,3] -> [0,1] [2,3] -> [0,1,2,3]
    auto ctx = make_context(4);

    // First fully split
    ctx->try_split_group(l0::group_id{0}); // [0,1] [2,3]
    ctx->try_split_group(l0::group_id{0}); // [0] [1] [2,3]
    ctx->try_split_group(l0::group_id{2}); // [0] [1] [2] [3]

    // Verify fully split state
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{1}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{2}), 1);
    EXPECT_EQ(ctx->get_group_size(l0::group_id{3}), 1);

    // Merge [2] with [3] -> [2,3]
    EXPECT_TRUE(ctx->try_merge_group(l0::group_id{2}));
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{2});
    EXPECT_EQ(ctx->get_group_size(l0::group_id{2}), 2);

    // Merge [0] with [1] -> [0,1]
    EXPECT_TRUE(ctx->try_merge_group(l0::group_id{0}));
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 2);

    // Merge [0,1] with [2,3] -> [0,1,2,3]
    EXPECT_TRUE(ctx->try_merge_group(l0::group_id{0}));
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 4);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(ctx->shard_to_group[i].load(), l0::group_id{0});
    }
}

TEST_F(scheduler_context_test, test_get_group_bytes) {
    auto ctx = make_context(4);

    // Set different backlog sizes
    set_backlog(0, 100);
    set_backlog(1, 200);
    set_backlog(2, 300);
    set_backlog(3, 400);

    // All in group 0, total should be 1000
    EXPECT_EQ(ctx->get_group_bytes(l0::group_id{0}), 1000);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});

    // Group 0: 100 + 200 = 300
    EXPECT_EQ(ctx->get_group_bytes(l0::group_id{0}), 300);
    // Group 2: 300 + 400 = 700
    EXPECT_EQ(ctx->get_group_bytes(l0::group_id{2}), 700);
}

TEST_F(scheduler_context_test, test_get_group_next_stage_bytes) {
    auto ctx = make_context(4);

    // Set next stage backlog sizes
    set_next_stage_backlog(0, 1000);
    set_next_stage_backlog(1, 2000);
    set_next_stage_backlog(2, 3000);
    set_next_stage_backlog(3, 4000);

    // All in group 0, total should be 10000
    EXPECT_EQ(ctx->get_group_next_stage_bytes(l0::group_id{0}), 10000);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});

    // Group 0: 1000 + 2000 = 3000
    EXPECT_EQ(ctx->get_group_next_stage_bytes(l0::group_id{0}), 3000);
    // Group 2: 3000 + 4000 = 7000
    EXPECT_EQ(ctx->get_group_next_stage_bytes(l0::group_id{2}), 7000);
}

TEST_F(scheduler_context_test, test_shard_index_in_group) {
    auto ctx = make_context(4);

    // All in group 0
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(0), l0::group_id{0}), 0);
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(1), l0::group_id{0}), 1);
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(2), l0::group_id{0}), 2);
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(3), l0::group_id{0}), 3);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});

    // Group 0: shard 0 is index 0, shard 1 is index 1
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(0), l0::group_id{0}), 0);
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(1), l0::group_id{0}), 1);

    // Group 2: shard 2 is index 0, shard 3 is index 1
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(2), l0::group_id{2}), 0);
    EXPECT_EQ(
      ctx->get_shard_index_in_group(ss::shard_id(3), l0::group_id{2}), 1);
}

TEST_F(scheduler_context_test, test_try_schedule_upload_triggers_split) {
    auto ctx = make_context(4);

    // Group has 4 shards, max_buffer_size = 4 MiB
    // Split threshold = 2 × 4 MiB × 4 = 32 MiB
    constexpr size_t max_buffer_size = 4 * 1024 * 1024; // 4 MiB
    constexpr size_t expected_split_threshold = 2 * max_buffer_size
                                                * 4; // 32 MiB

    // Set high next stage backlog to trigger split
    set_next_stage_backlog(0, expected_split_threshold + 1);
    set_next_stage_backlog(1, expected_split_threshold + 1);
    set_next_stage_backlog(2, expected_split_threshold + 1);
    set_next_stage_backlog(3, expected_split_threshold + 1);

    // Set some backlog to trigger upload
    set_backlog(0, 1000);

    // Advance time past the cooldown period (2 * scheduling_interval = 200ms)
    ss::manual_clock::advance(250ms);

    // Shard 0 is first in group, should evaluate and trigger split
    auto result = ctx->try_schedule_upload(
      ss::shard_id(0), max_buffer_size, 100ms, clock_type::now(), probe);

    // After split, shards 0,1 should be in group 0, shards 2,3 in group 2
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{2});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{2});
}

TEST_F(scheduler_context_test, test_try_schedule_upload_triggers_merge) {
    auto ctx = make_context(4);

    // First split the groups
    ctx->try_split_group(l0::group_id{0}); // [0,1] [2,3]

    // Set next stage backlog to 0 to trigger merge
    set_next_stage_backlog(0, 0);
    set_next_stage_backlog(1, 0);
    set_next_stage_backlog(2, 0);
    set_next_stage_backlog(3, 0);

    // Set some backlog
    set_backlog(0, 1000);

    // Advance time past the cooldown period (2 * scheduling_interval = 200ms)
    // for both groups (split sets last_modification_time on both)
    ss::manual_clock::advance(250ms);

    // Shard 0 is first in group 0, should evaluate and trigger merge
    auto result = ctx->try_schedule_upload(
      ss::shard_id(0),
      500, // max_buffer_size
      100ms,
      clock_type::now(),
      probe);

    // After merge, all shards should be in group 0
    EXPECT_EQ(ctx->shard_to_group[0].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[1].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[2].load(), l0::group_id{0});
    EXPECT_EQ(ctx->shard_to_group[3].load(), l0::group_id{0});
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), 4);
}

TEST_F(scheduler_context_test, test_round_robin_within_group) {
    auto ctx = make_context(4);

    // Split into [0,1] and [2,3]
    ctx->try_split_group(l0::group_id{0});

    // Set backlog so upload conditions are met
    set_backlog(0, 1000);
    set_backlog(1, 1000);

    // Round-robin counter starts at 0, so shard 0 should be selected first
    auto result0 = ctx->try_schedule_upload(
      ss::shard_id(0), 500, 100ms, clock_type::now(), probe);
    EXPECT_EQ(result0.action, l0::schedule_action::upload);

    // Release the lock before checking next shard (simulates upload completion)
    result0.request.reset();

    // After upload, ix is incremented, so shard 1 should be selected next
    auto result1_skip = ctx->try_schedule_upload(
      ss::shard_id(0), 500, 100ms, clock_type::now(), probe);
    EXPECT_EQ(result1_skip.action, l0::schedule_action::skip);

    auto result1 = ctx->try_schedule_upload(
      ss::shard_id(1), 500, 100ms, clock_type::now(), probe);
    EXPECT_EQ(result1.action, l0::schedule_action::upload);
}

// ============================================================================
// Fuzz test for scheduler_context
// ============================================================================

class scheduler_context_fuzz_test : public testing::Test {
public:
    using clock_type = ss::manual_clock;

    static constexpr size_t num_shards = 8;
    static constexpr size_t max_buffer_size = 4 * 1024 * 1024; // 4 MiB
    // Split threshold is now dynamic: max_buffer_size × group_size
    // For initial group of 8 shards: 4 MiB × 8 = 32 MiB
    static constexpr auto scheduling_interval = 100ms;
    static constexpr auto iteration_interval = 20ms;
    static constexpr auto upload_interval = 250ms;

    // Mock backlog counters - one per shard
    std::vector<std::unique_ptr<std::atomic<size_t>>> backlog_counters;
    std::vector<std::unique_ptr<std::atomic<size_t>>> next_stage_counters;

    // Random number generator
    std::mt19937 rng;

    scheduler_context_fuzz_test()
      : rng(std::random_device{}()) {}

    std::unique_ptr<l0::scheduler_context<clock_type>> make_context() {
        backlog_counters.clear();
        next_stage_counters.clear();

        for (size_t i = 0; i < num_shards; i++) {
            backlog_counters.push_back(
              std::make_unique<std::atomic<size_t>>(0));
            next_stage_counters.push_back(
              std::make_unique<std::atomic<size_t>>(0));
        }

        // FixedArrays are sized at construction time
        auto ctx = std::make_unique<l0::scheduler_context<clock_type>>(
          num_shards);

        // Initialize shards with references to our mock counters.
        // shard_to_group and groups are already default-initialized.
        for (size_t i = 0; i < num_shards; i++) {
            ctx->shards[i] = l0::shard_state<clock_type>(
              std::ref(*backlog_counters[i]),
              std::ref(
                static_cast<const std::atomic<size_t>&>(
                  *next_stage_counters[i])));
        }

        return ctx;
    }

    size_t get_backlog(size_t shard) const {
        return backlog_counters[shard]->load();
    }

    void set_backlog(size_t shard, size_t bytes) {
        backlog_counters[shard]->store(bytes);
    }

    void add_backlog(size_t shard, size_t bytes) {
        backlog_counters[shard]->fetch_add(bytes);
    }

    size_t get_next_stage_backlog(size_t shard) const {
        return next_stage_counters[shard]->load();
    }

    void set_next_stage_backlog(size_t shard, size_t bytes) {
        next_stage_counters[shard]->store(bytes);
    }

    void add_next_stage_backlog(size_t shard, size_t bytes) {
        next_stage_counters[shard]->fetch_add(bytes);
    }

    l0::write_request_scheduler_probe probe{true};

    // Simulate ingestion: add random bytes to current stage backlog
    void simulate_ingestion() {
        std::uniform_int_distribution<size_t> bytes_dist(0, 512 * 1024);
        for (size_t shard = 0; shard < num_shards; shard++) {
            // 70% chance to add data on each shard
            if (std::uniform_int_distribution<int>(0, 9)(rng) < 7) {
                add_backlog(shard, bytes_dist(rng));
            }
        }
    }

    // Simulate upload completion: drain next stage backlog
    void simulate_upload_drain() {
        for (size_t shard = 0; shard < num_shards; shard++) {
            set_next_stage_backlog(shard, 0);
        }
    }

    // Simulate partial upload completion: reduce next stage backlog by 10%
    void simulate_partial_upload_drain() {
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto current = get_next_stage_backlog(shard);
            auto reduction = current / 10;
            if (reduction > 0) {
                next_stage_counters[shard]->fetch_sub(reduction);
            }
        }
    }

    // Deposit bytes from current stage to next stage (simulates upload
    // starting)
    void deposit_to_next_stage(
      l0::scheduler_context<clock_type>& ctx, l0::group_id gid) {
        for (size_t shard = 0; shard < num_shards; shard++) {
            if (ctx.shard_to_group[shard].load() != gid) {
                continue;
            }
            auto bytes = get_backlog(shard);
            if (bytes > 0) {
                set_backlog(shard, 0);
                add_next_stage_backlog(shard, bytes);
            }
        }
    }

    // Verify invariants
    void verify_invariants(const l0::scheduler_context<clock_type>& ctx) {
        // Every shard must belong to a valid group
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto gid = ctx.shard_to_group[shard].load();
            // Group ID must be <= shard ID (buddy allocator invariant)
            ASSERT_LE(static_cast<size_t>(gid), shard)
              << "Shard " << shard << " has invalid group_id " << gid;
        }

        // Groups must be contiguous
        for (size_t shard = 1; shard < num_shards; shard++) {
            auto prev_gid = ctx.shard_to_group[shard - 1].load();
            auto curr_gid = ctx.shard_to_group[shard].load();
            // Either same group or new group starting at this shard
            ASSERT_TRUE(
              prev_gid == curr_gid || static_cast<size_t>(curr_gid) == shard)
              << "Non-contiguous group at shard " << shard;
        }

        // Total bytes should be consistent
        size_t total_backlog = 0;
        size_t total_next_stage = 0;
        for (size_t shard = 0; shard < num_shards; shard++) {
            total_backlog += get_backlog(shard);
            total_next_stage += get_next_stage_backlog(shard);
        }
        // Just verify we can compute these without crash
        (void)total_backlog;
        (void)total_next_stage;
    }
};

TEST_F(scheduler_context_fuzz_test, test_random_workload) {
    auto ctx = make_context();

    // Track pending uploads per group (group_id -> lock)
    std::map<uint32_t, std::optional<std::unique_lock<std::mutex>>>
      pending_uploads;

    // Advance time past the cooldown period (2 * scheduling_interval = 200ms)
    // so split/merge can happen in the first iteration
    ss::manual_clock::advance(250ms);
    auto simulated_time = clock_type::now();
    auto last_upload_drain_time = simulated_time;

    // Run for 1000 iterations (20 seconds of simulated time)
    for (int iteration = 0; iteration < 1000; iteration++) {
        // Verify invariants at start of each iteration
        verify_invariants(*ctx);

        // Step 1: Simulate ingestion (add bytes to current stage)
        simulate_ingestion();

        // Step 2: Randomly modify next stage backlog
        int next_stage_action = std::uniform_int_distribution<int>(0, 9)(rng);
        if (next_stage_action == 0) {
            // 10% chance: drop next stage backlog to 0 (upload completed)
            simulate_upload_drain();
        } else if (next_stage_action == 1) {
            // 10% chance: reduce next stage backlog by 10%
            simulate_partial_upload_drain();
        }
        // 80% chance: keep next stage backlog unchanged

        // Step 3: Run scheduler on every shard
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto result = ctx->try_schedule_upload(
              ss::shard_id(shard),
              max_buffer_size,
              scheduling_interval,
              simulated_time,
              probe);

            if (result.action == l0::schedule_action::upload) {
                // Deposit bytes from current stage to next stage
                deposit_to_next_stage(*ctx, result.gid);

                // Record upload time
                ctx->record_upload_time(result.gid, simulated_time);

                // Release lock immediately (simulates quick upload)
                result.request.reset();
            }
        }

        // Step 4: Simulate upload drain every 250ms
        if (simulated_time - last_upload_drain_time >= upload_interval) {
            simulate_upload_drain();
            last_upload_drain_time = simulated_time;
        }

        // Step 5: Advance simulated time
        ss::manual_clock::advance(iteration_interval);
        simulated_time = clock_type::now();
    }

    // Final invariant check
    verify_invariants(*ctx);
}

TEST_F(scheduler_context_fuzz_test, test_high_load_causes_splits) {
    auto ctx = make_context();

    // Verify all shards start in group 0
    EXPECT_EQ(ctx->get_group_size(l0::group_id{0}), num_shards);

    // Advance time past the cooldown period (2 * scheduling_interval = 200ms)
    // so splits can happen in the first iteration
    ss::manual_clock::advance(250ms);
    auto simulated_time = clock_type::now();

    // Simulate high load: lots of data, slow drain
    for (int iteration = 0; iteration < 500; iteration++) {
        verify_invariants(*ctx);

        // Add significant data to all shards
        for (size_t shard = 0; shard < num_shards; shard++) {
            add_backlog(shard, 1024 * 1024); // 1 MiB per shard per iteration
        }

        // Run scheduler on every shard
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto result = ctx->try_schedule_upload(
              ss::shard_id(shard),
              max_buffer_size,
              scheduling_interval,
              simulated_time,
              probe);

            if (result.action == l0::schedule_action::upload) {
                deposit_to_next_stage(*ctx, result.gid);
                ctx->record_upload_time(result.gid, simulated_time);
                result.request.reset();
            }
        }

        // Slow drain: only 5% reduction every iteration
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto current = get_next_stage_backlog(shard);
            auto reduction = current / 20;
            if (reduction > 0) {
                next_stage_counters[shard]->fetch_sub(reduction);
            }
        }

        ss::manual_clock::advance(iteration_interval);
        simulated_time = clock_type::now();
    }

    verify_invariants(*ctx);

    // Under high load with slow drain, groups should have split
    // Count number of distinct groups
    std::set<uint32_t> distinct_groups;
    for (size_t shard = 0; shard < num_shards; shard++) {
        distinct_groups.insert(
          static_cast<uint32_t>(ctx->shard_to_group[shard].load()));
    }
    EXPECT_GT(distinct_groups.size(), 1)
      << "Expected groups to split under high load";
}

TEST_F(scheduler_context_fuzz_test, test_low_load_causes_merges) {
    auto ctx = make_context();

    // First, force split into individual shards
    while (ctx->get_group_size(l0::group_id{0}) > 1) {
        ctx->try_split_group(l0::group_id{0});
    }
    // Split remaining groups
    for (size_t shard = 0; shard < num_shards; shard++) {
        auto gid = ctx->shard_to_group[shard].load();
        while (ctx->get_group_size(gid) > 1) {
            ctx->try_split_group(gid);
        }
    }

    // Verify all shards are in their own group
    for (size_t shard = 0; shard < num_shards; shard++) {
        EXPECT_EQ(ctx->get_group_size(ctx->shard_to_group[shard].load()), 1);
    }

    // Advance time past the cooldown period (2 * scheduling_interval = 200ms)
    // so merges can happen in the first iteration
    ss::manual_clock::advance(250ms);
    auto simulated_time = clock_type::now();

    // Simulate low load: minimal data, fast drain
    for (int iteration = 0; iteration < 500; iteration++) {
        verify_invariants(*ctx);

        // Add minimal data
        for (size_t shard = 0; shard < num_shards; shard++) {
            if (std::uniform_int_distribution<int>(0, 4)(rng) == 0) {
                add_backlog(shard, 64 * 1024); // 64 KiB occasionally
            }
        }

        // Fast drain: clear next stage frequently
        if (iteration % 5 == 0) {
            simulate_upload_drain();
        }

        // Run scheduler on every shard
        for (size_t shard = 0; shard < num_shards; shard++) {
            auto result = ctx->try_schedule_upload(
              ss::shard_id(shard),
              max_buffer_size,
              scheduling_interval,
              simulated_time,
              probe);

            if (result.action == l0::schedule_action::upload) {
                deposit_to_next_stage(*ctx, result.gid);
                ctx->record_upload_time(result.gid, simulated_time);
                result.request.reset();
            }
        }

        ss::manual_clock::advance(iteration_interval);
        simulated_time = clock_type::now();
    }

    verify_invariants(*ctx);

    // Under low load with fast drain, groups should have merged
    // Count number of distinct groups
    std::set<uint32_t> distinct_groups;
    for (size_t shard = 0; shard < num_shards; shard++) {
        distinct_groups.insert(
          static_cast<uint32_t>(ctx->shard_to_group[shard].load()));
    }
    EXPECT_LT(distinct_groups.size(), num_shards)
      << "Expected groups to merge under low load";
}
