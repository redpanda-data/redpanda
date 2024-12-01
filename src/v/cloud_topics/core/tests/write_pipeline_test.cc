// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/core/write_pipeline.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <tuple>

namespace cloud_topics = experimental::cloud_topics;
using namespace std::chrono_literals;

namespace experimental::cloud_topics::core {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) {
        return pipeline->_pending.size() == n;
    }

    write_pipeline<ss::manual_clock>* pipeline;
};
} // namespace experimental::cloud_topics::core

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
    cloud_topics::core::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::core::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };
    auto reader = model::make_empty_record_batch_reader();
    // Expect single upload to be made

    std::ignore = pipeline.register_pipeline_stage();

    const auto timeout = 1s;
    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader), timeout);

    // Make sure the write request is in the _pending list
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    auto res = pipeline.get_write_requests(
      1, cloud_topics::core::unassigned_pipeline_stage);
    ASSERT_TRUE_CORO(res.complete);
    ASSERT_TRUE_CORO(res.size_bytes == 0);
    ASSERT_TRUE_CORO(res.ready.size() == 1);

    res.ready.front().set_value(ss::circular_buffer<model::record_batch>{});

    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());
}

TEST_CORO(batcher_test, expired_write_request) {
    // The test starts two write request but one of which is expected to
    // timeout.
    cloud_topics::core::write_pipeline<ss::manual_clock> pipeline;
    cloud_topics::core::write_pipeline_accessor accessor{
      .pipeline = &pipeline,
    };

    std::ignore = pipeline.register_pipeline_stage();

    const auto timeout = 1s;
    auto expect_fail_fut = pipeline.write_and_debounce(
      model::controller_ntp, model::make_empty_record_batch_reader(), timeout);

    // Expire first request
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });
    ss::manual_clock::advance(timeout);

    auto expect_pass_fut = pipeline.write_and_debounce(
      model::controller_ntp, model::make_empty_record_batch_reader(), timeout);

    // Make sure that both write requests are pending
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(2); });

    auto res = pipeline.get_write_requests(
      1, cloud_topics::core::unassigned_pipeline_stage);

    // One req has already expired at this point
    ASSERT_EQ_CORO(res.ready.size(), 1);
    res.ready.back().set_value(ss::circular_buffer<model::record_batch>{});

    auto [pass_result, fail_result] = co_await ss::when_all_succeed(
      std::move(expect_pass_fut), std::move(expect_fail_fut));

    ASSERT_TRUE_CORO(fail_result.has_error());

    ASSERT_TRUE_CORO(pass_result.has_value());
}
