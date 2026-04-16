/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <iterator>
#include <limits>
#include <tuple>

using namespace std::chrono_literals;

static cloud_topics::cluster_epoch min_epoch{3840};

namespace cloud_topics::l0 {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) {
        return pipeline->get_pending().size() == n;
    }

    // Manually add a write request to the pending list with a specific stage
    void add_request_with_stage(
      write_request<ss::manual_clock>& req, pipeline_stage stage) {
        req.stage = stage;
        pipeline->get_pending().push_back(req);
        pipeline->signal(stage);
    }

    write_pipeline<ss::manual_clock>* pipeline;
};
} // namespace cloud_topics::l0

// Simulate sleep of certain duration and wait until the condition is met
template<class Fn>
ss::future<>
sleep_until(std::chrono::milliseconds delta, Fn&& fn, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
        if (fn()) {
            co_return;
        }
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

TEST_CORO(write_pipeline_test, single_write_request) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };
    // Expect single upload to be made

    auto stage = pipeline.register_write_pipeline_stage();

    const auto timeout = ss::manual_clock::now() + 1s;
    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, timeout);

    // Make sure the write request is in the _pending list
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    auto res = stage.pull_write_requests(1);
    ASSERT_TRUE_CORO(res.complete);
    ASSERT_TRUE_CORO(res.requests.size() == 1);

    res.requests.front().set_value(cloud_topics::upload_meta{});

    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());
}

TEST_CORO(batcher_test, expired_write_request) {
    // The test starts two write request but one of which is expected to
    // timeout.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    static constexpr auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + 1s;
    auto expect_fail_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, deadline);

    // Expire first request
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });
    ss::manual_clock::advance(timeout);

    deadline = ss::manual_clock::now() + 1s;
    auto expect_pass_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, {}, deadline);

    // Make sure that both write requests are pending
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(2); });

    auto res = stage.pull_write_requests(1);

    // One req has already expired at this point
    ASSERT_EQ_CORO(res.requests.size(), 1);
    res.requests.back().set_value(cloud_topics::upload_meta{});

    auto [pass_result, fail_result] = co_await ss::when_all_succeed(
      std::move(expect_pass_fut), std::move(expect_fail_fut));

    ASSERT_TRUE_CORO(!fail_result.has_value());

    ASSERT_TRUE_CORO(pass_result.has_value());
}

TEST_CORO(write_pipeline_test, stage_bytes_accounting) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    const auto timeout = ss::manual_clock::now() + 1s;
    auto test_data = co_await model::test::make_random_batches(
      {.count = 1, .records = 5});
    chunked_vector<model::record_batch> batches;
    std::ranges::move(std::move(test_data), std::back_inserter(batches));

    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(batches), timeout);

    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    // Bytes in the stage.
    ASSERT_GT_CORO(pipeline.stage_bytes(stage.id()), 0);

    // Pull requests -- bytes leave the stage.
    auto res = stage.pull_write_requests(std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    res.requests.front().set_value(cloud_topics::upload_meta{});
    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());
}

TEST_CORO(write_pipeline_test, stage_bytes_accounting_on_timeout) {
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);

    static constexpr auto timeout_duration = 1s;
    const auto timeout = ss::manual_clock::now() + timeout_duration;
    auto test_data = co_await model::test::make_random_batches(
      {.count = 1, .records = 5});
    chunked_vector<model::record_batch> batches;
    std::ranges::move(std::move(test_data), std::back_inserter(batches));

    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(batches), timeout);

    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    // Bytes in the stage.
    ASSERT_GT_CORO(pipeline.stage_bytes(stage.id()), 0);

    // "Wait" until the requests time out.
    ss::manual_clock::advance(timeout_duration);

    // Trigger timeout detection by pulling requests.
    auto res = stage.pull_write_requests(std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(res.requests.size(), 0);

    // Stage bytes are released when the future resolves.
    auto write_res = co_await std::move(fut);
    ASSERT_FALSE_CORO(write_res.has_value());
    ASSERT_EQ_CORO(pipeline.stage_bytes(stage.id()), 0);
}

TEST_CORO(write_pipeline_test, interleaving_stages_bug) {
    // This test demonstrates a bug where get_write_requests can return
    // requests that belong to the wrong pipeline stage when requests
    // with different stages are interleaved.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage1 = pipeline.register_write_pipeline_stage();
    auto stage2 = pipeline.register_write_pipeline_stage();

    const auto timeout = ss::manual_clock::now() + 10s;

    auto test_data = co_await model::test::make_random_batches(
      {.count = 1, .records = 5});

    // Helper to create a serialized chunk from test data
    auto make_chunk = [&]() -> ss::future<cloud_topics::l0::serialized_chunk> {
        chunked_vector<model::record_batch> batches;
        auto data = co_await model::test::make_random_batches(
          {.count = 1, .records = 5});
        std::ranges::move(std::move(data), std::back_inserter(batches));
        co_return co_await cloud_topics::l0::serialize_batches(
          std::move(batches));
    };

    // Create 6 write requests with interleaved stages:
    // stage1, stage2, stage1, stage2, stage1, stage2
    std::vector<
      std::unique_ptr<cloud_topics::l0::write_request<ss::manual_clock>>>
      requests;

    for (int i = 0; i < 6; i++) {
        auto chunk = co_await make_chunk();
        auto req
          = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
            model::controller_ntp, min_epoch, std::move(chunk), timeout);
        requests.push_back(std::move(req));
    }

    // Add requests with interleaved stages
    accessor.add_request_with_stage(*requests[0], stage1.id());
    accessor.add_request_with_stage(*requests[1], stage2.id());
    accessor.add_request_with_stage(*requests[2], stage1.id());
    accessor.add_request_with_stage(*requests[3], stage2.id());
    accessor.add_request_with_stage(*requests[4], stage1.id());
    accessor.add_request_with_stage(*requests[5], stage2.id());

    co_await ss::yield();

    ASSERT_EQ_CORO(accessor.write_requests_pending(6), true);

    // Try to get requests for stage1 only - should get exactly 3 requests
    auto result = stage1.pull_write_requests(
      std::numeric_limits<size_t>::max());

    ASSERT_EQ_CORO(result.requests.size(), 3);

    for (auto& req : result.requests) {
        // Stage should be unassigned after extraction
        ASSERT_TRUE_CORO(
          req.stage == cloud_topics::l0::unassigned_pipeline_stage);
        req.set_value(cloud_topics::upload_meta{});
    }

    // The remaining 3 requests should still be in the pending queue
    ASSERT_TRUE_CORO(accessor.write_requests_pending(3));

    // Get stage2 requests - should get exactly 3 requests
    auto result2 = stage2.pull_write_requests(
      std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(result2.requests.size(), 3);

    for (auto& req : result2.requests) {
        ASSERT_TRUE_CORO(
          req.stage == cloud_topics::l0::unassigned_pipeline_stage);
        req.set_value(cloud_topics::upload_meta{});
    }

    ASSERT_EQ_CORO(accessor.write_requests_pending(0), true);
}

TEST_CORO(write_pipeline_test, oversized_request) {
    // This test verifies that get_write_requests returns at least one
    // request even if it exceeds the max_bytes limit, to prevent pipeline
    // stalls with oversized requests.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();
    const auto timeout = ss::manual_clock::now() + 10s;

    // Helper to create a serialized chunk with specific size
    auto make_chunk_with_size =
      [&](
        size_t target_size) -> ss::future<cloud_topics::l0::serialized_chunk> {
        // Create batches until we reach approximately the target size
        chunked_vector<model::record_batch> batches;
        size_t current_size = 0;
        while (current_size < target_size) {
            auto data = co_await model::test::make_random_batches(
              {.count = 1, .records = 10});
            for (auto& batch : data) {
                current_size += batch.size_bytes();
                batches.push_back(std::move(batch));
                if (current_size >= target_size) {
                    break;
                }
            }
        }
        co_return co_await cloud_topics::l0::serialize_batches(
          std::move(batches));
    };

    std::vector<
      std::unique_ptr<cloud_topics::l0::write_request<ss::manual_clock>>>
      requests;

    // First request is oversized (approximately 10000 bytes)
    auto chunk1 = co_await make_chunk_with_size(10000);
    auto req1
      = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
        model::controller_ntp, min_epoch, std::move(chunk1), timeout);
    requests.push_back(std::move(req1));

    // Second request is normal size (approximately 1000 bytes)
    auto chunk2 = co_await make_chunk_with_size(1000);
    auto req2
      = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
        model::controller_ntp, min_epoch, std::move(chunk2), timeout);
    requests.push_back(std::move(req2));

    // Add both requests to stage
    accessor.add_request_with_stage(*requests[0], stage.id());
    accessor.add_request_with_stage(*requests[1], stage.id());

    co_await ss::yield();

    ASSERT_TRUE_CORO(accessor.write_requests_pending(2));

    // Try to get requests with max_bytes = 5000 (less than first request)
    // Should still get the first request to avoid stalling
    auto result = stage.pull_write_requests(5000);

    // Should return exactly 1 request (the oversized one)
    ASSERT_EQ_CORO(result.requests.size(), 1);
    result.requests.front().set_value(cloud_topics::upload_meta{});

    // Second request should still be pending
    ASSERT_EQ_CORO(accessor.write_requests_pending(1), true);

    // Get the second request
    auto result2 = stage.pull_write_requests(
      std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(result2.requests.size(), 1);
    result2.requests.front().set_value(cloud_topics::upload_meta{});

    ASSERT_EQ_CORO(accessor.write_requests_pending(0), true);
}

TEST_CORO(write_pipeline_test, multiple_requests_within_limit) {
    // This test verifies that get_write_requests returns multiple requests
    // when they fit within the size limit.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();
    const auto timeout = ss::manual_clock::now() + 10s;

    // Helper to create a serialized chunk with specific size
    auto make_chunk_with_size =
      [&](
        size_t target_size) -> ss::future<cloud_topics::l0::serialized_chunk> {
        chunked_vector<model::record_batch> batches;
        size_t current_size = 0;
        while (current_size < target_size) {
            auto data = co_await model::test::make_random_batches(
              {.count = 1, .records = 5});
            for (auto& batch : data) {
                current_size += batch.size_bytes();
                batches.push_back(std::move(batch));
                if (current_size >= target_size) {
                    break;
                }
            }
        }
        co_return co_await cloud_topics::l0::serialize_batches(
          std::move(batches));
    };

    // Create 3 requests with size approximately 1000 each
    std::vector<
      std::unique_ptr<cloud_topics::l0::write_request<ss::manual_clock>>>
      requests;

    for (int i = 0; i < 3; i++) {
        auto chunk = co_await make_chunk_with_size(1000);
        auto req
          = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
            model::controller_ntp, min_epoch, std::move(chunk), timeout);
        requests.push_back(std::move(req));
    }

    // Add all requests to stage
    for (auto& req : requests) {
        accessor.add_request_with_stage(*req, stage.id());
    }

    co_await ss::yield();

    ASSERT_TRUE_CORO(accessor.write_requests_pending(3));

    // Get requests with a large max_bytes to get all 3 initially
    // Then use max_bytes to get a subset in a second call
    auto result_all = stage.pull_write_requests(
      std::numeric_limits<size_t>::max());

    // Should return all 3 requests
    ASSERT_EQ_CORO(result_all.requests.size(), 3);

    for (auto& req : result_all.requests) {
        req.set_value(cloud_topics::upload_meta{});
    }

    ASSERT_TRUE_CORO(accessor.write_requests_pending(0));
}

TEST_CORO(write_pipeline_test, max_requests_limit) {
    // This test verifies that get_write_requests respects the max_requests
    // parameter, but always returns at least one request.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_write_pipeline_stage();
    const auto timeout = ss::manual_clock::now() + 10s;

    auto make_chunk = [&]() -> ss::future<cloud_topics::l0::serialized_chunk> {
        chunked_vector<model::record_batch> batches;
        auto data = co_await model::test::make_random_batches(
          {.count = 1, .records = 5});
        std::ranges::move(std::move(data), std::back_inserter(batches));
        co_return co_await cloud_topics::l0::serialize_batches(
          std::move(batches));
    };

    // Create 5 small requests
    std::vector<
      std::unique_ptr<cloud_topics::l0::write_request<ss::manual_clock>>>
      requests;

    for (int i = 0; i < 5; i++) {
        auto chunk = co_await make_chunk();
        auto req
          = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
            model::controller_ntp, min_epoch, std::move(chunk), timeout);
        requests.push_back(std::move(req));
    }

    // Add all requests to stage
    for (auto& req : requests) {
        accessor.add_request_with_stage(*req, stage.id());
    }

    co_await ss::yield();

    ASSERT_EQ_CORO(accessor.write_requests_pending(5), true);

    // Get requests with max_requests = 3
    auto result = stage.pull_write_requests(
      std::numeric_limits<size_t>::max(), 3);

    // Should return exactly 3 requests
    // Note: Due to the "first request always included" logic, we get at least 1
    ASSERT_GE_CORO(result.requests.size(), 1);
    ASSERT_LE_CORO(result.requests.size(), 3);

    size_t first_batch_size = result.requests.size();

    for (auto& req : result.requests) {
        req.set_value(cloud_topics::upload_meta{});
    }

    // Remaining requests should still be pending
    ASSERT_EQ_CORO(accessor.write_requests_pending(5 - first_batch_size), true);

    // Get the remaining requests
    auto result2 = stage.pull_write_requests(
      std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(result2.requests.size(), 5 - first_batch_size);

    for (auto& req : result2.requests) {
        req.set_value(cloud_topics::upload_meta{});
    }

    ASSERT_TRUE_CORO(accessor.write_requests_pending(0));
}

TEST_CORO(write_pipeline_test, enqueue_foreign_request_accounts_bytes) {
    // Verify that enqueue_foreign_request updates _stage_bytes for
    // the destination stage. This was previously missing, causing
    // the scheduler to underreport cross-shard work.
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;

    auto stage1 = pipeline.register_write_pipeline_stage();
    auto stage2 = pipeline.register_write_pipeline_stage();

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage2.id()), 0);

    const auto timeout = ss::manual_clock::now() + 10s;

    auto make_chunk = [&]() -> ss::future<cloud_topics::l0::serialized_chunk> {
        chunked_vector<model::record_batch> batches;
        auto data = co_await model::test::make_random_batches(
          {.count = 1, .records = 5});
        std::ranges::move(std::move(data), std::back_inserter(batches));
        co_return co_await cloud_topics::l0::serialize_batches(
          std::move(batches));
    };

    auto chunk = co_await make_chunk();
    auto req
      = std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
        model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto expected_size = req->size_bytes();

    // enqueue_foreign_request should account bytes at the next stage
    stage1.enqueue_foreign_request(*req, false);

    ASSERT_EQ_CORO(pipeline.stage_bytes(stage2.id()), expected_size);

    // Pull from stage2 — bytes should be released
    auto res = stage2.pull_write_requests(std::numeric_limits<size_t>::max());
    ASSERT_EQ_CORO(res.requests.size(), 1);
    ASSERT_EQ_CORO(pipeline.stage_bytes(stage2.id()), 0);

    res.requests.front().set_value(cloud_topics::upload_meta{});
}
