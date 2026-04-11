// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/read_merge/read_merge.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <expected>
#include <limits>

using namespace std::chrono_literals;

static ss::logger test_log("L0_read_merge_test");

namespace cloud_topics {

/// Manual fetch handler that gives the test full control over when
/// and how requests are fulfilled.
struct fetch_handler {
    explicit fetch_handler(l0::read_pipeline<ss::manual_clock>& p)
      : stage(p.register_read_pipeline_stage()) {}

    using read_requests_list = l0::requests_list<
      l0::read_pipeline<ss::manual_clock>,
      l0::read_request<ss::manual_clock>>;

    ss::future<std::expected<read_requests_list, errc>> get_next_requests() {
        auto result = co_await stage.pull_fetch_requests(
          std::numeric_limits<size_t>::max());

        if (!result.has_value()) {
            co_return std::unexpected(result.error());
        }

        auto list = std::move(result.value());
        co_return std::move(list);
    }

    l0::read_pipeline<ss::manual_clock>::stage stage;
};

} // namespace cloud_topics

using namespace cloud_topics;

class read_merge_fixture : public seastar_test {
public:
    ss::future<> start() {
        co_await pipeline.start();

        co_await merge.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        co_await merge.invoke_on_all(
          [](l0::read_merge<ss::manual_clock>& s) { return s.start(); });

        co_await sink.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));
    }

    ss::future<> stop() {
        co_await pipeline.invoke_on_all([](auto& s) { return s.shutdown(); });
        co_await sink.stop();
        co_await merge.stop();
        co_await pipeline.stop();
    }

    /// Helper: make a query targeting a specific object_id.
    static l0::dataplane_query make_query(object_id id, size_t size = 1_MiB) {
        l0::dataplane_query query;
        query.output_size_estimate = size;
        query.meta.push_back(
          extent_meta{.id = id, .byte_range_size = byte_range_size_t{size}});
        return query;
    }

    /// Helper: make a single random batch result.
    static chunked_vector<model::record_batch> make_batch_result() {
        chunked_vector<model::record_batch> batches;
        model::test::record_batch_spec spec{};
        spec.offset = model::offset{0};
        spec.count = 1;
        spec.records = 1;
        batches.push_back(model::test::make_random_batch(spec));
        return batches;
    }

    ss::sharded<l0::read_pipeline<ss::manual_clock>> pipeline;
    ss::sharded<l0::read_merge<ss::manual_clock>> merge;
    ss::sharded<fetch_handler> sink;
};

static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));

static const model::ntp
  test_ntp(test_topic.ns, test_topic.tp, model::partition_id(0));

TEST_F_CORO(read_merge_fixture, test_happy_path) {
    // A single request goes through read_merge and gets a result.
    co_await start();

    auto id = object_id{.name = uuid_t::create()};
    auto result_fut = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto request = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request.has_value());
    ASSERT_EQ_CORO(request.value().requests.size(), 1);

    request.value().requests.front().set_value({{make_batch_result()}});

    auto result = co_await std::move(result_fut);
    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.value().results.size(), 1);
    ASSERT_EQ_CORO(
      result.value().results.front().header().base_offset, model::offset{0});

    co_await stop();
}

TEST_F_CORO(read_merge_fixture, test_error_propagation) {
    // Error from the fetch handler is propagated to the caller.
    co_await start();

    auto id = object_id{.name = uuid_t::create()};
    auto result_fut = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto request = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request.has_value());
    ASSERT_EQ_CORO(request.value().requests.size(), 1);

    request.value().requests.front().set_value(errc::timeout);

    auto result = co_await std::move(result_fut);
    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), errc::timeout);

    co_await stop();
}

TEST_F_CORO(read_merge_fixture, test_merge_same_object) {
    // Two concurrent requests for the same object_id: only one proxy
    // reaches the fetch handler. The first caller gets the actual result
    // forwarded. The second caller gets pushed to the next pipeline stage
    // after the first completes (where it would hit cache in production).
    co_await start();

    auto id = object_id{.name = uuid_t::create()};

    // Launch two concurrent requests for the same object.
    auto fut1 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto fut2 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    // The fetch handler should only receive ONE proxy request (the first).
    auto request = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request.has_value());
    ASSERT_EQ_CORO(request.value().requests.size(), 1);

    // Fulfill the proxy — this signals the first caller AND wakes the
    // second caller's shared_future.
    request.value().requests.front().set_value({{make_batch_result()}});

    // The first request should get the actual result.
    auto result1 = co_await std::move(fut1);
    ASSERT_TRUE_CORO(result1.has_value());
    ASSERT_EQ_CORO(result1.value().results.size(), 1);

    // The second request was pushed to the next stage after the first
    // completed. The fetch handler should now receive it.
    auto request2 = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request2.has_value());
    ASSERT_EQ_CORO(request2.value().requests.size(), 1);

    // Fulfill the second request (simulates cache hit).
    request2.value().requests.front().set_value({{make_batch_result()}});

    auto result2 = co_await std::move(fut2);
    ASSERT_TRUE_CORO(result2.has_value());
    ASSERT_EQ_CORO(result2.value().results.size(), 1);

    co_await stop();
}

TEST_F_CORO(read_merge_fixture, test_merge_error_propagated_to_waiters) {
    // When the first request fails, waiting requests should get the
    // error propagated instead of retrying independently.
    co_await start();

    auto id = object_id{.name = uuid_t::create()};

    auto fut1 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto fut2 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    // Only one proxy reaches the fetch handler.
    auto request = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request.has_value());
    ASSERT_EQ_CORO(request.value().requests.size(), 1);

    // Fail the proxy request.
    request.value().requests.front().set_value(errc::timeout);

    // The first caller gets the error from the proxy.
    auto result1 = co_await std::move(fut1);
    ASSERT_FALSE_CORO(result1.has_value());
    ASSERT_EQ_CORO(result1.error(), errc::timeout);

    // The second caller should also get the error (propagated via
    // shared_promise), not be pushed to the next stage.
    auto result2 = co_await std::move(fut2);
    ASSERT_FALSE_CORO(result2.has_value());
    ASSERT_EQ_CORO(result2.error(), errc::timeout);

    co_await stop();
}

TEST_F_CORO(read_merge_fixture, test_unique_objects_not_merged) {
    // Requests for different object_ids should NOT be merged — each
    // gets its own proxy to the fetch handler.
    co_await start();

    auto id1 = object_id{.name = uuid_t::create()};
    auto id2 = object_id{.name = uuid_t::create()};

    auto fut1 = pipeline.local().make_reader(
      test_ntp, make_query(id1), ss::manual_clock::now() + 10s);

    auto fut2 = pipeline.local().make_reader(
      test_ntp, make_query(id2), ss::manual_clock::now() + 10s);

    // The fetch handler should receive TWO proxy requests (one per object).
    auto request = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request.has_value());
    ASSERT_EQ_CORO(request.value().requests.size(), 2);

    // Fulfill both.
    for (auto& r : request.value().requests) {
        r.set_value({{make_batch_result()}});
    }

    auto result1 = co_await std::move(fut1);
    ASSERT_TRUE_CORO(result1.has_value());
    ASSERT_EQ_CORO(result1.value().results.size(), 1);

    auto result2 = co_await std::move(fut2);
    ASSERT_TRUE_CORO(result2.has_value());
    ASSERT_EQ_CORO(result2.value().results.size(), 1);

    co_await stop();
}

TEST_F_CORO(read_merge_fixture, test_second_request_after_first_completes) {
    // After the first request for an object completes, a new request
    // for the same object should get its own proxy (not merged) since
    // the in-flight entry was cleaned up.
    co_await start();

    auto id = object_id{.name = uuid_t::create()};

    // First request.
    auto fut1 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto request1 = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request1.has_value());
    ASSERT_EQ_CORO(request1.value().requests.size(), 1);

    request1.value().requests.front().set_value({{make_batch_result()}});

    auto result1 = co_await std::move(fut1);
    ASSERT_TRUE_CORO(result1.has_value());

    // Second request for the same id — should get its own proxy since
    // the first one completed and was removed from _in_flight.
    auto fut2 = pipeline.local().make_reader(
      test_ntp, make_query(id), ss::manual_clock::now() + 10s);

    auto request2 = co_await sink.local().get_next_requests();
    ASSERT_TRUE_CORO(request2.has_value());
    ASSERT_EQ_CORO(request2.value().requests.size(), 1);

    request2.value().requests.front().set_value({{make_batch_result()}});

    auto result2 = co_await std::move(fut2);
    ASSERT_TRUE_CORO(result2.has_value());

    co_await stop();
}
