// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/evictor/evictor.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <limits>

inline ss::logger test_log("evictor_gtest");

namespace cloud_topics = experimental::cloud_topics;
using namespace std::chrono_literals;

namespace experimental::cloud_topics {

struct evictor_metrics {
    size_t events_counter;
    size_t requests_evicted;
};

struct evictor_accessor {
    ss::future<result<size_t>> run_once(size_t prev) noexcept {
        return evictor->evict_once(prev);
    }

    auto metrics() const {
        return evictor_metrics{
          .events_counter = evictor->_total_events,
          .requests_evicted = evictor->_throttle_by_mem,
        };
    }

    cloud_topics::evictor* evictor;
};
} // namespace experimental::cloud_topics

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

    // Ack all write requests in the pipeline
    void ack_all() {
        std::vector<core::pipeline_stage> stages;
        auto stage = unassigned_pipeline_stage;
        for (int i = 0; i < pipeline->_next_stage_id_to_alloc; i++) {
            stage = pipeline->next_stage(stage);
            stages.emplace_back(stage);
        }
        for (auto stage : stages) {
            auto list0 = pipeline->get_write_requests(
              std::numeric_limits<size_t>::max(), stage);
            for (auto& r : list0.ready) {
                r.set_value(ss::circular_buffer<model::record_batch>());
            }
        }
    }

    cloud_topics::core::write_pipeline<>* pipeline;
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
ss::future<> sleep(std::chrono::milliseconds delay, Fn&& fn) {
    auto start = ss::lowres_clock::now();
    while (!fn()) {
        co_await ss::sleep(1ms);
        if (ss::lowres_clock::now() > start + delay) {
            GTEST_MESSAGE_(
              "Test stalled", ::testing::TestPartResult::kFatalFailure);
        }
    }
}

TEST_CORO(EvictorTest, no_eviction) {
    // Normal operation, eviction shouldn't affect request processing
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches = co_await model::test::make_random_batches(spec);
    size_t reader_size_bytes = get_serialized_size(batches);
    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    cloud_topics::core::write_pipeline<> pipeline;

    size_t memory_limit = std::numeric_limits<size_t>::max();

    vlog(
      test_log.info,
      "Creating evictor, expected input size: {}, memory limit: {}",
      reader_size_bytes,
      memory_limit);

    cloud_topics::evictor evictor(memory_limit, pipeline);
    cloud_topics::evictor_accessor evictor_accessor{
      .evictor = &evictor,
    };

    cloud_topics::core::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    vlog(
      test_log.info,
      "Register pipeline sink: {}",
      pipeline.register_pipeline_stage());

    // This fut will become ready when something will be added to
    // the pipeline.
    auto evict_fut = evictor_accessor.run_once(0);
    co_await ss::sleep(1ms);

    // This fut will become ready when something will get the write
    // request from the pipeline and acknowledge it.
    auto write_fut = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader), 1s);

    co_await sleep(100ms, [pipeline_accessor] {
        return pipeline_accessor.write_requests_pending(1);
    });

    pipeline_accessor.ack_all();

    auto write_res = co_await std::move(write_fut);
    ASSERT_TRUE_CORO(write_res.has_value());

    auto evictor_res = co_await std::move(evict_fut);
    ASSERT_TRUE_CORO(evictor_res.has_value());
}

TEST_CORO(EvictorTest, mem_limit_reached) {
    // First request should be
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    auto batches1 = co_await model::test::make_random_batches(spec);
    decltype(batches1) batches2;
    for (const auto& b : batches1) {
        batches2.push_back(b.copy());
    }
    size_t reader_size_bytes = get_serialized_size(batches1);

    // Both readers have the same size
    auto reader1 = model::make_memory_record_batch_reader(std::move(batches1));
    auto reader2 = model::make_memory_record_batch_reader(std::move(batches2));

    cloud_topics::core::write_pipeline<> pipeline;

    size_t memory_limit = reader_size_bytes;

    vlog(
      test_log.info,
      "Creating evictor, expected input size: {}, memory limit: {}",
      reader_size_bytes,
      memory_limit);

    cloud_topics::evictor evictor(memory_limit, pipeline);
    cloud_topics::evictor_accessor evictor_accessor{
      .evictor = &evictor,
    };
    cloud_topics::core::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    vlog(
      test_log.info,
      "Register pipeline sink: {}",
      pipeline.register_pipeline_stage());

    // This will not trigger evictor because 'run_once' is not
    // called yet.
    vlog(test_log.info, "Writing first request");
    auto write_fut1 = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader1), 1s);

    co_await sleep(100ms, [pipeline_accessor] {
        return pipeline_accessor.write_requests_pending(1);
    });

    vlog(test_log.info, "Writing second request");
    auto write_fut2 = pipeline.write_and_debounce(
      model::controller_ntp, std::move(reader2), 1s);

    co_await sleep(100ms, [pipeline_accessor] {
        return pipeline_accessor.write_requests_pending(2);
    });

    vlog(test_log.info, "Start eviction");
    auto evict_res = co_await evictor_accessor.run_once(0);

    if (evict_res.has_value()) {
        vlog(test_log.info, "Evictor future result: {}", evict_res.value());
    }
    ASSERT_TRUE_CORO(evict_res.has_value());
    ASSERT_EQ_CORO(evictor_accessor.metrics().events_counter, 1);
    ASSERT_EQ_CORO(evictor_accessor.metrics().requests_evicted, 1);

    pipeline_accessor.ack_all();

    auto write_res1 = co_await std::move(write_fut1);
    ASSERT_TRUE_CORO(write_res1.has_error());
    ASSERT_TRUE_CORO(write_res1.error() == cloud_topics::errc::slow_down);

    auto write_res2 = co_await std::move(write_fut2);
    ASSERT_TRUE_CORO(write_res2.has_value());

    co_await evictor.stop();
}

TEST_CORO(EvictorTest, graceful_shutdown) {
    // Eviction should shutdown gracefully
    model::test::record_batch_spec spec{
      .offset = model::offset{0},
      .allow_compression = false,
      .count = 100,
      .records = 10,
    };
    cloud_topics::core::write_pipeline<> pipeline;

    size_t memory_limit = 100;

    cloud_topics::evictor evictor(memory_limit, pipeline);

    vlog(
      test_log.info,
      "Register pipeline sink: {}",
      pipeline.register_pipeline_stage());

    cloud_topics::evictor_accessor evictor_accessor{
      .evictor = &evictor,
    };

    // This fut will become ready when something will be added to
    // the pipeline. We're going to stop the evictor instead.
    auto evictor_fut = evictor_accessor.run_once(0);
    co_await ss::sleep(1ms);

    co_await evictor.stop();

    auto evictor_res = co_await std::move(evictor_fut);
    ASSERT_TRUE_CORO(evictor_res.has_error());
    ASSERT_EQ_CORO(evictor_res.error(), cloud_topics::errc::shutting_down);
}
