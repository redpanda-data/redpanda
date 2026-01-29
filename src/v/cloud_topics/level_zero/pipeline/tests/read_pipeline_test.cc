/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {
struct read_pipeline_accessor {
    // Returns true if the read request is in the `_pending` collection
    bool read_requests_pending(size_t n) {
        return pipeline->get_pending().size() == n;
    }

    // Manually add a read request to the pending list with a specific stage
    void add_request_with_stage(
      read_request<ss::manual_clock>& req, pipeline_stage stage) {
        req.stage = stage;
        pipeline->get_pending().push_back(req);
        pipeline->signal(stage);
    }

    // Call get_fetch_requests (which is private)
    read_pipeline<ss::manual_clock>::read_requests_list
    get_fetch_requests(size_t max_bytes, pipeline_stage stage) {
        return pipeline->get_fetch_requests(max_bytes, stage);
    }

    read_pipeline<ss::manual_clock>* pipeline;
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

TEST_CORO(read_pipeline_test, interleaving_stages_bug) {
    // This test demonstrates a bug where get_fetch_requests can return
    // requests that belong to the wrong pipeline stage when requests
    // with different stages are interleaved.
    cloud_topics::l0::read_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::read_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage1 = pipeline.register_read_pipeline_stage();
    auto stage2 = pipeline.register_read_pipeline_stage();

    const auto timeout = ss::manual_clock::now() + 10s;

    // Create 6 read requests with interleaved stages:
    // stage1, stage2, stage1, stage2, stage1, stage2
    std::vector<
      std::unique_ptr<cloud_topics::l0::read_request<ss::manual_clock>>>
      requests;

    for (int i = 0; i < 6; i++) {
        cloud_topics::l0::dataplane_query query;
        query.output_size_estimate = 1000 + i * 100; // Different sizes
        auto req
          = std::make_unique<cloud_topics::l0::read_request<ss::manual_clock>>(
            model::controller_ntp,
            std::move(query),
            timeout,
            &pipeline.get_root_rtc());
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

    ASSERT_EQ_CORO(accessor.read_requests_pending(6), true);

    // Try to get requests for stage1 only - should get exactly 3 requests
    auto result = accessor.get_fetch_requests(
      std::numeric_limits<size_t>::max(), stage1.id());

    ASSERT_EQ_CORO(result.requests.size(), 3);

    for (auto& req : result.requests) {
        req.set_value(cloud_topics::l0::dataplane_query_result{});
    }

    // The remaining 3 requests should still be in the pending queue
    ASSERT_EQ_CORO(accessor.read_requests_pending(3), true);

    // Get stage2 requests - should get exactly 3 requests
    auto result2 = accessor.get_fetch_requests(
      std::numeric_limits<size_t>::max(), stage2.id());
    ASSERT_EQ_CORO(result2.requests.size(), 3);

    for (auto& req : result2.requests) {
        req.set_value(cloud_topics::l0::dataplane_query_result{});
    }

    ASSERT_EQ_CORO(accessor.read_requests_pending(0), true);
}

TEST_CORO(read_pipeline_test, oversized_request) {
    // This test verifies that get_fetch_requests returns at least one
    // request even if it exceeds the max_bytes limit, to prevent pipeline
    // stalls with oversized requests.
    cloud_topics::l0::read_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::read_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_read_pipeline_stage();
    const auto timeout = ss::manual_clock::now() + 10s;

    // Create requests with different sizes
    std::vector<
      std::unique_ptr<cloud_topics::l0::read_request<ss::manual_clock>>>
      requests;

    // First request is oversized (10000 bytes)
    cloud_topics::l0::dataplane_query query1;
    query1.output_size_estimate = 10000;
    auto req1
      = std::make_unique<cloud_topics::l0::read_request<ss::manual_clock>>(
        model::controller_ntp,
        std::move(query1),
        timeout,
        &pipeline.get_root_rtc());
    requests.push_back(std::move(req1));

    // Second request is normal size (1000 bytes)
    cloud_topics::l0::dataplane_query query2;
    query2.output_size_estimate = 1000;
    auto req2
      = std::make_unique<cloud_topics::l0::read_request<ss::manual_clock>>(
        model::controller_ntp,
        std::move(query2),
        timeout,
        &pipeline.get_root_rtc());
    requests.push_back(std::move(req2));

    // Add both requests to stage
    accessor.add_request_with_stage(*requests[0], stage.id());
    accessor.add_request_with_stage(*requests[1], stage.id());

    co_await ss::yield();

    ASSERT_EQ_CORO(accessor.read_requests_pending(2), true);

    // Try to get requests with max_bytes = 5000 (less than first request)
    // Should still get the first request to avoid stalling
    auto result = accessor.get_fetch_requests(5000, stage.id());

    // Should return exactly 1 request (the oversized one)
    ASSERT_EQ_CORO(result.requests.size(), 1);
    result.requests.front().set_value(
      cloud_topics::l0::dataplane_query_result{});

    // Second request should still be pending
    ASSERT_EQ_CORO(accessor.read_requests_pending(1), true);

    // Get the second request
    auto result2 = accessor.get_fetch_requests(
      std::numeric_limits<size_t>::max(), stage.id());
    ASSERT_EQ_CORO(result2.requests.size(), 1);
    result2.requests.front().set_value(
      cloud_topics::l0::dataplane_query_result{});

    ASSERT_EQ_CORO(accessor.read_requests_pending(0), true);
}

TEST_CORO(read_pipeline_test, multiple_requests_within_limit) {
    // This test verifies that get_fetch_requests returns multiple requests
    // when they fit within the size limit.
    cloud_topics::l0::read_pipeline<ss::manual_clock> pipeline;
    cloud_topics::l0::read_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    auto stage = pipeline.register_read_pipeline_stage();
    const auto timeout = ss::manual_clock::now() + 10s;

    // Create 3 requests with size 1000 each
    std::vector<
      std::unique_ptr<cloud_topics::l0::read_request<ss::manual_clock>>>
      requests;

    for (int i = 0; i < 3; i++) {
        cloud_topics::l0::dataplane_query query;
        query.output_size_estimate = 1000;
        auto req
          = std::make_unique<cloud_topics::l0::read_request<ss::manual_clock>>(
            model::controller_ntp,
            std::move(query),
            timeout,
            &pipeline.get_root_rtc());
        requests.push_back(std::move(req));
    }

    // Add all requests to stage
    for (auto& req : requests) {
        accessor.add_request_with_stage(*req, stage.id());
    }

    co_await ss::yield();

    ASSERT_EQ_CORO(accessor.read_requests_pending(3), true);

    // Get requests with max_bytes = 2500 (should get 2 requests, not 3)
    auto result = accessor.get_fetch_requests(2500, stage.id());

    // Should return 2 requests (total 2000 bytes, within limit)
    ASSERT_EQ_CORO(result.requests.size(), 2);

    for (auto& req : result.requests) {
        req.set_value(cloud_topics::l0::dataplane_query_result{});
    }

    // One request should still be pending
    ASSERT_EQ_CORO(accessor.read_requests_pending(1), true);

    // Get the last request
    auto result2 = accessor.get_fetch_requests(
      std::numeric_limits<size_t>::max(), stage.id());
    ASSERT_EQ_CORO(result2.requests.size(), 1);
    result2.requests.front().set_value(
      cloud_topics::l0::dataplane_query_result{});

    ASSERT_EQ_CORO(accessor.read_requests_pending(0), true);
}
