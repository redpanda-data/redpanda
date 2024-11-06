/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/core/write_pipeline.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <limits>

using namespace experimental::cloud_topics;
using namespace std::chrono_literals;

namespace experimental::cloud_topics::core {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) const {
        return pipeline->_pending.size() == n;
    }

    // Returns true if `_filters` collection has right amount of filters
    bool event_filters_subscribed(size_t n) const {
        return pipeline->_filters.size() == n;
    }

    core::write_pipeline<ss::lowres_clock>* pipeline;
};
} // namespace experimental::cloud_topics::core

size_t get_serialized_size(const model::record_batch& rb) {
    size_t res = model::packed_record_batch_header_size;
    res += rb.copy().release_data().size_bytes();
    return res;
}

size_t
get_serialized_size(const ss::circular_buffer<model::record_batch>& batches) {
    size_t acc = 0;
    for (const auto& rb : batches) {
        auto sz = get_serialized_size(rb);
        acc += sz;
    }
    return acc;
}

template<class Fn>
ss::future<> do_until(Fn&& fn, int retries = 100) {
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
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    core::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_pipeline_stage();
    core::event_filter<ss::lowres_clock> flt(
      core::event_type::new_write_request, stage);
    auto sub = pipeline.subscribe(flt);
    auto write = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader), 1s);
    auto event = co_await std::move(sub);
    ASSERT_EQ_CORO(event.pending_write_bytes, reader_size_bytes);
    auto pending = pipeline.get_write_requests(
      std::numeric_limits<size_t>::max(), stage);
    for (auto& req : pending.ready) {
        req.set_value(ss::circular_buffer<model::record_batch>());
    }
    std::ignore = co_await std::move(write);
    co_return;
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
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    core::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_pipeline_stage();
    auto write = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader), 1s);
    // wait until submitted
    // this is needed because 'write_and_debounce' has a scheduling point
    // before the write request is added to the list
    co_await do_until([&pipeline] {
        core::write_pipeline_accessor accessor{.pipeline = &pipeline};
        return accessor.write_requests_pending(1);
    });
    core::event_filter<ss::lowres_clock> flt(
      core::event_type::new_write_request, stage);
    auto event = co_await pipeline.subscribe(flt);
    ASSERT_EQ_CORO(event.pending_write_bytes, reader_size_bytes);
    auto pending = pipeline.get_write_requests(
      std::numeric_limits<size_t>::max(), stage);
    for (auto& req : pending.ready) {
        req.set_value(ss::circular_buffer<model::record_batch>());
    }
    std::ignore = co_await std::move(write);
    co_return;
}

TEST_CORO(EventFilterTest, filter_shutdown) {
    core::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_pipeline_stage();
    // The subscription mechanism uses external abort source
    core::event_filter<ss::lowres_clock> flt(
      core::event_type::new_write_request, stage);
    ss::abort_source as;
    auto sub = pipeline.subscribe(flt, as);
    co_await ss::sleep(10ms);
    as.request_abort();
    auto res = co_await std::move(sub);
    ASSERT_TRUE_CORO(res.type == core::event_type::shutting_down);
    co_return;
}

TEST_CORO(EventFilterTest, filter_timedout) {
    core::write_pipeline<ss::lowres_clock> pipeline;
    auto stage = pipeline.register_pipeline_stage();
    // The subscription mechanism can be aborted by timeout
    core::event_filter<ss::lowres_clock> flt(
      core::event_type::new_write_request,
      stage,
      ss::lowres_clock::now() + 1ms);
    auto sub = pipeline.subscribe(flt);
    co_await ss::sleep(10ms);
    auto res = co_await std::move(sub);
    ASSERT_TRUE_CORO(res.type == core::event_type::err_timedout);
    co_return;
}
