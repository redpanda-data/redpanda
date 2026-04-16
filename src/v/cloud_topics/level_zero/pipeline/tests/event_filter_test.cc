/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <limits>

using namespace cloud_topics;
using namespace std::chrono_literals;

static cloud_topics::cluster_epoch min_epoch{3840};

namespace cloud_topics::l0 {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) const {
        return pipeline->get_pending().size() == n;
    }

    // Returns true if `_filters` collection has right amount of filters
    bool event_filters_subscribed(size_t n) const {
        return pipeline->get_filters().size() == n;
    }

    l0::write_pipeline<ss::lowres_clock>* pipeline;
};
} // namespace cloud_topics::l0

size_t get_serialized_size(const model::record_batch& rb) {
    size_t res = model::packed_record_batch_header_size;
    res += rb.copy().release_data().size_bytes();
    return res;
}

size_t get_serialized_size(
  const chunked_circular_buffer<model::record_batch>& batches) {
    size_t acc = 0;
    for (const auto& rb : batches) {
        auto sz = get_serialized_size(rb);
        acc += sz;
    }
    return acc;
}

template<class Fn>
ss::future<> do_until(Fn&& fn, int retries = 100) {
    co_await ss::sleep(1ms);
    for (int i = 0; i < retries; i++) {
        if (fn()) {
            co_return;
        }
        co_await ss::yield();
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

template<class Fn>
ss::future<> do_until(Fn&& fn, std::chrono::milliseconds timeout) {
    auto start = ss::lowres_clock::now();
    while (start + timeout > ss::lowres_clock::now()) {
        if (fn()) {
            co_return;
        }
        co_await ss::sleep(10ms);
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

TEST_CORO(EventFilterTest, filter_triggered_once) {
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = co_await model::test::make_random_batches(spec);
    size_t reader_size_bytes = get_serialized_size(batches);
    l0::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_write_pipeline_stage();
    l0::event_filter<ss::lowres_clock> flt(
      l0::event_type::new_write_request, stage.id());
    auto sub = pipeline.subscribe(flt);
    auto write = pipeline.write_and_debounce(
      model::controller_ntp,
      min_epoch,
      chunked_vector<model::record_batch>(
        std::from_range, std::move(batches) | std::views::as_rvalue),
      ss::lowres_clock::now() + 1s);
    auto event = co_await std::move(sub);
    ASSERT_EQ_CORO(event.pending_write_bytes, reader_size_bytes);
    auto pending = stage.pull_write_requests(
      std::numeric_limits<size_t>::max());
    for (auto& req : pending.requests) {
        req.set_value(upload_meta{});
    }
    std::ignore = co_await std::move(write);
}

TEST_CORO(EventFilterTest, filter_has_memory) {
    // Check that the ordering between the write request submission
    // and filter subscription doesn't matter. Previous test checks
    // a situation when the subscription is created first and then
    // the write request is submitted. This test checks the opposite.
    // The write request is submitted and then the write request is
    // submitted.
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = co_await model::test::make_random_batches(spec);
    size_t reader_size_bytes = get_serialized_size(batches);
    l0::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_write_pipeline_stage();
    auto write = pipeline.write_and_debounce(
      model::controller_ntp,
      min_epoch,
      chunked_vector<model::record_batch>(
        std::from_range, std::move(batches) | std::views::as_rvalue),
      ss::lowres_clock::now() + 1s);
    // wait until submitted
    // this is needed because 'write_and_debounce' has a scheduling point
    // before the write request is added to the list
    co_await do_until([&pipeline] {
        l0::write_pipeline_accessor accessor{.pipeline = &pipeline};
        return accessor.write_requests_pending(1);
    });
    l0::event_filter<ss::lowres_clock> flt(
      l0::event_type::new_write_request, stage.id());
    auto event = co_await pipeline.subscribe(flt);
    ASSERT_EQ_CORO(event.pending_write_bytes, reader_size_bytes);
    auto pending = stage.pull_write_requests(
      std::numeric_limits<size_t>::max());
    for (auto& req : pending.requests) {
        req.set_value(upload_meta{});
    }
    std::ignore = co_await std::move(write);
    co_return;
}

TEST_CORO(EventFilterTest, filter_shutdown) {
    l0::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_write_pipeline_stage();
    // The subscription mechanism uses external abort source
    l0::event_filter<ss::lowres_clock> flt(
      l0::event_type::new_write_request, stage.id());
    ss::abort_source as;
    auto sub = pipeline.subscribe(flt, as);
    co_await ss::sleep(10ms);
    as.request_abort();
    auto res = co_await std::move(sub);
    ASSERT_TRUE_CORO(res.type == l0::event_type::shutting_down);
    co_return;
}

TEST_CORO(EventFilterTest, filter_timedout) {
    l0::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_write_pipeline_stage();
    // The subscription mechanism can be aborted by timeout
    l0::event_filter<ss::lowres_clock> flt(
      l0::event_type::new_write_request,
      stage.id(),
      ss::lowres_clock::now() + 1ms,
      {});
    auto sub = pipeline.subscribe(flt);
    co_await ss::sleep(10ms);
    auto res = co_await std::move(sub);
    ASSERT_TRUE_CORO(res.type == l0::event_type::err_timedout);
    co_return;
}

TEST_CORO(EventFilterTest, filter_min_write_bytes) {
    auto batch1 = model::test::make_random_batch({
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    });
    auto batch2 = model::test::make_random_batch({
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    });
    l0::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_write_pipeline_stage();
    // The subscription mechanism can be aborted by timeout
    l0::event_filter<ss::lowres_clock> flt(
      l0::event_type::new_write_request,
      stage.id(),
      ss::lowres_clock::now() + 10000s,
      {.min_pending_write_bytes = get_serialized_size(batch1)
                                  + get_serialized_size(batch2)});
    auto sub = pipeline.subscribe(flt);
    EXPECT_FALSE(sub.available());
    auto write1 = pipeline.write_and_debounce(
      model::controller_ntp,
      min_epoch,
      chunked_vector<model::record_batch>::single(batch1.copy()),
      ss::lowres_clock::now() + 1s);
    co_await tests::drain_task_queue();
    EXPECT_FALSE(sub.available());
    auto write2 = pipeline.write_and_debounce(
      model::controller_ntp,
      min_epoch,
      chunked_vector<model::record_batch>::single(batch2.copy()),
      ss::lowres_clock::now() + 1s);
    co_await tests::drain_task_queue();
    auto res = co_await std::move(sub);
    ASSERT_TRUE_CORO(res.type == l0::event_type::new_write_request);
    auto pending = stage.pull_write_requests(
      std::numeric_limits<size_t>::max());
    for (auto& req : pending.requests) {
        req.set_value(upload_meta{});
    }
    std::ignore = co_await std::move(write1);
    std::ignore = co_await std::move(write2);
}
