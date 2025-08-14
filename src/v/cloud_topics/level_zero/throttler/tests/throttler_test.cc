/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/throttler/throttler.h"
#include "container/chunked_circular_buffer.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <limits>

inline ss::logger test_log("throttler_gtest");

namespace cloud_topics = experimental::cloud_topics;
using namespace std::chrono_literals;

namespace experimental::cloud_topics::l0 {

struct throttler_metrics {
    size_t events_counter;
    size_t times_tput_throttled;
    size_t outstanding_throttled_requests;
};

struct throttler_accessor {
    ss::future<result<size_t>> run_once(size_t prev) noexcept {
        return throttler->throttle_write_pipeline_once(prev);
    }

    size_t units_available() const {
        return throttler->_write_tput_tb.available();
    }

    auto metrics() const {
        return throttler_metrics{
          .events_counter = throttler->_total_events,
          .times_tput_throttled = throttler->_throttle_by_tput,
          .outstanding_throttled_requests
          = throttler->_outstanding_throttled_requests,
        };
    }

    throttler<ss::manual_clock>* throttler;
};
} // namespace experimental::cloud_topics::l0

namespace experimental::cloud_topics::l0 {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) const {
        return pipeline->get_pending().size() == n;
    }

    // Returns true if `_filters` collection has right amount of filters
    bool event_filters_subscribed(size_t n) const {
        return pipeline->get_filters().size() == n;
    }

    // Ack all write requests in the pipeline
    void ack_all() {
        std::vector<l0::pipeline_stage> stages;
        auto stage = unassigned_pipeline_stage;
        for (int i = 0; i < 10; i++) {
            stage = pipeline->next_stage(stage);
            stages.emplace_back(stage);
        }
        for (auto stage : stages) {
            auto list0 = pipeline->get_write_requests(
              std::numeric_limits<size_t>::max(), stage);
            for (auto& r : list0.requests) {
                r.set_value(chunked_vector<extent_meta>());
            }
        }
    }

    cloud_topics::l0::write_pipeline<ss::manual_clock>* pipeline;
};
} // namespace experimental::cloud_topics::l0

ss::future<> sleep(std::chrono::milliseconds delta, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    co_await ss::sleep(1ms);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
    }
}

// Simulate sleep of certain duration and wait until the condition is met
template<class Fn>
ss::future<>
sleep_until(std::chrono::milliseconds delta, Fn&& fn, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    co_await ss::sleep(1ms);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
        if (fn()) {
            co_return;
        }
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

size_t get_serialized_size(const model::record_batch& rb) {
    size_t res = model::packed_record_batch_header_size;
    res += rb.copy().release_data().size_bytes();
    return res;
}

size_t get_serialized_size(const chunked_vector<model::record_batch>& batches) {
    size_t acc = 0;
    for (const auto& rb : batches) {
        auto sz = get_serialized_size(rb);
        acc += sz;
    }
    return acc;
}

TEST_CORO(throttler_test, no_throttling) {
    // Normal operation, throttling shouldn't affect request processing
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = chunked_vector<model::record_batch>(
      std::from_range,
      co_await model::test::make_random_batches(spec) | std::views::as_rvalue);
    size_t reader_size_bytes = get_serialized_size(batches);

    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;

    size_t tput_limit = reader_size_bytes * 2;

    vlog(
      test_log.info,
      "Creating throttler, expected input size: {}, throughput limit: {}",
      reader_size_bytes,
      tput_limit);

    cloud_topics::l0::throttler<ss::manual_clock> throttler(
      tput_limit, pipeline.register_write_pipeline_stage());
    cloud_topics::l0::throttler_accessor throttler_accessor{
      .throttler = &throttler,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };
    std::ignore = pipeline.register_write_pipeline_stage();
    ASSERT_EQ_CORO(throttler_accessor.units_available(), tput_limit);

    // This fut will become ready when something will be added to
    // the pipeline.
    auto throttle_fut = throttler_accessor.run_once(0);
    co_await sleep_until(1ms, [pipeline_accessor] {
        return pipeline_accessor.event_filters_subscribed(1);
    });

    // This fut will become ready when something will get the write
    // request from the pipeline and acknowledge it.
    auto write_fut = pipeline.write_and_debounce(
      model::controller_ntp, std::move(batches), ss::manual_clock::now() + 1s);
    co_await sleep_until(1ms, [throttler_accessor, reader_size_bytes] {
        return throttler_accessor.units_available() == reader_size_bytes;
    });

    ASSERT_EQ_CORO(throttler_accessor.units_available(), reader_size_bytes);
    pipeline_accessor.ack_all();

    auto write_res = co_await std::move(write_fut);
    ASSERT_TRUE_CORO(write_res.has_value());

    auto throttle_res = co_await std::move(throttle_fut);
    ASSERT_TRUE_CORO(throttle_res.has_value());

    co_await throttler.stop();
    co_await pipeline.stop();
}

TEST_CORO(throttler_test, tput_limit_reached) {
    // The first and only write request uses the tput limit and
    // should be throttled.
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = chunked_vector<model::record_batch>(
      std::from_range,
      co_await model::test::make_random_batches(spec) | std::views::as_rvalue);
    size_t reader_size_bytes = get_serialized_size(batches);

    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;

    size_t tput_limit = reader_size_bytes / 2;

    vlog(
      test_log.info,
      "Creating throttler, expected input size: {}, throughput limit: {}",
      reader_size_bytes,
      tput_limit);

    cloud_topics::l0::throttler<ss::manual_clock> throttler(
      tput_limit, pipeline.register_write_pipeline_stage());
    cloud_topics::l0::throttler_accessor throttler_accessor{
      .throttler = &throttler,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    std::ignore = pipeline.register_write_pipeline_stage();

    auto throttle_fut = throttler_accessor.run_once(0);
    co_await sleep_until(1ms, [pipeline_accessor] {
        return pipeline_accessor.event_filters_subscribed(1);
    });

    // This will trigger throttler because the size of the reader
    // is greater than tput
    vlog(test_log.info, "Writing first request");
    auto write_fut = pipeline.write_and_debounce(
      model::controller_ntp, std::move(batches), ss::manual_clock::now() + 10s);

    //  This should move the write request out of the pipeline
    auto throttle_res = co_await std::move(throttle_fut);
    ASSERT_TRUE_CORO(throttle_res.has_value());
    ASSERT_TRUE_CORO(pipeline_accessor.write_requests_pending(0));

    // After being throttled the write request should be back
    // in the write pipeline after about a second
    co_await sleep_until(2s, [pipeline_accessor, throttler_accessor] {
        vlog(
          test_log.debug,
          "Waiting for the TB, units available: {}",
          throttler_accessor.units_available());
        return pipeline_accessor.write_requests_pending(1);
    });

    pipeline_accessor.ack_all();

    // The write request is not timed out at this point because
    // timeout is longer than throttling period
    auto write_res = co_await std::move(write_fut);
    ASSERT_TRUE_CORO(write_res.has_value());

    co_await throttler.stop();
    co_await pipeline.stop();
}

TEST_CORO(throttler_test, tput_limit_reached_req_timed_out) {
    // The first and only write request uses the tput limit and
    // should be throttled. The request times out while being
    // throttled.
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = chunked_vector<model::record_batch>(
      std::from_range,
      co_await model::test::make_random_batches(spec) | std::views::as_rvalue);
    size_t reader_size_bytes = get_serialized_size(batches);

    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;

    size_t tput_limit = reader_size_bytes / 2;

    vlog(
      test_log.info,
      "Creating throttler, expected input size: {}, throughput limit: {}",
      reader_size_bytes,
      tput_limit);

    cloud_topics::l0::throttler<ss::manual_clock> throttler(
      tput_limit, pipeline.register_write_pipeline_stage());
    cloud_topics::l0::throttler_accessor throttler_accessor{
      .throttler = &throttler,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    std::ignore = pipeline.register_write_pipeline_stage();

    auto throttle_fut = throttler_accessor.run_once(0);
    co_await sleep_until(1ms, [pipeline_accessor] {
        return pipeline_accessor.event_filters_subscribed(1);
    });

    // This will trigger throttler because the size of the reader
    // is greater than tput. The timeout is lower than the throttling
    // that will be applied.
    auto write_fut = pipeline.write_and_debounce(
      model::controller_ntp,
      std::move(batches),
      ss::manual_clock::now() + 200ms);

    // This should move the write request out of the pipeline.
    // It should stay there up until it times out.
    auto throttle_res = co_await std::move(throttle_fut);
    ASSERT_TRUE_CORO(throttle_res.has_value());
    ASSERT_TRUE_CORO(pipeline_accessor.write_requests_pending(0));

    // After being throttled the write request should eventually
    // timeout and acknowledged with errc::timeout
    ss::manual_clock::advance(2s);

    // The write request is not timed out at this point because
    // timeout is longer than throttling period
    auto write_res = co_await std::move(write_fut);
    ASSERT_TRUE_CORO(write_res.has_error());
    ASSERT_TRUE_CORO(write_res.error() == cloud_topics::errc::timeout);

    co_await throttler.stop();
    co_await pipeline.stop();
}

TEST_CORO(throttler_test, graceful_shutdown) {
    // Throttling should shutdown gracefully
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;

    size_t tput_limit = 100;

    cloud_topics::l0::throttler<ss::manual_clock> throttler(
      tput_limit, pipeline.register_write_pipeline_stage());

    std::ignore = pipeline.register_write_pipeline_stage();

    cloud_topics::l0::throttler_accessor throttler_accessor{
      .throttler = &throttler,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };
    ASSERT_EQ_CORO(throttler_accessor.units_available(), tput_limit);

    // This fut will become ready when something will be added to
    // the pipeline. We're going to stop the throttler instead.
    auto throttle_fut = throttler_accessor.run_once(0);
    co_await sleep_until(1ms, [pipeline_accessor] {
        return pipeline_accessor.event_filters_subscribed(1);
    });

    co_await throttler.stop();

    auto throttle_res = co_await std::move(throttle_fut);
    ASSERT_TRUE_CORO(throttle_res.has_error());
    ASSERT_EQ_CORO(throttle_res.error(), cloud_topics::errc::shutting_down);

    co_await pipeline.stop();
}
