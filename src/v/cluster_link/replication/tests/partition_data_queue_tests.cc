/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/replication/partition_data_queue.h"
#include "cluster_link/replication/types.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>

using namespace cluster_link::replication;
using namespace std::chrono_literals;

static ss::logger test_log("partition-data-queue-test");

class PartitionDataQueueTest : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        _queue = std::make_unique<partition_data_queue>(
          1024 * 1024 // 1MB buffer size
        );
        return ss::now();
    }

    ss::future<> TearDownAsync() override {
        if (_queue) {
            co_await _queue->stop();
        }
    }

    ss::future<bool> enqueue_some_data() {
        auto data = chunked_vector<model::record_batch>{
          std::from_range,
          co_await model::test::make_random_batches(model::offset{0}, 10, true)
            | std::views::as_rvalue};
        co_return _queue->enqueue(std::move(data));
    }

protected:
    std::unique_ptr<partition_data_queue> _queue;
};

TEST_F_CORO(PartitionDataQueueTest, testWaiterBehavior) {
    // waiter before enqueue
    ss::abort_source as;
    auto fetch_future = _queue->fetch(as);
    ASSERT_FALSE_CORO(fetch_future.available());
    co_await enqueue_some_data();
    co_await std::move(fetch_future).then([this](fetch_data data) {
        ASSERT_FALSE(data.batches.empty());
        ASSERT_TRUE(_queue->empty());
    });

    // enqueue empty batches
    fetch_future = _queue->fetch(as);
    ASSERT_FALSE_CORO(fetch_future.available());
    _queue->enqueue(chunked_vector<model::record_batch>{});
    co_await std::move(fetch_future).then([this](fetch_data data) {
        ASSERT_TRUE(data.batches.empty());
        ASSERT_TRUE(_queue->empty());
    });

    // enqueue before waiter
    co_await enqueue_some_data();
    ASSERT_FALSE_CORO(_queue->empty());
    fetch_future = _queue->fetch(as);
    co_await std::move(fetch_future).then([this](fetch_data data) {
        ASSERT_FALSE(data.batches.empty());
        ASSERT_TRUE(_queue->empty());
    });
    // double waiter should throw
    fetch_future = _queue->fetch(as);
    EXPECT_THROW(co_await _queue->fetch(as), std::runtime_error);

    // reset aborting the waiter
    _queue->reset(kafka::offset{0});
    EXPECT_THROW(
      co_await std::move(fetch_future), ss::abort_requested_exception);

    // check abort source behavior
    fetch_future = _queue->fetch(as);
    as.request_abort();
    EXPECT_THROW(
      co_await std::move(fetch_future), ss::abort_requested_exception);
}

TEST_F_CORO(PartitionDataQueueTest, testMemoryUsage) {
    while (!_queue->full()) {
        co_await enqueue_some_data();
    }
    // enqueue still works as the behavior is to oversubscribe
    ASSERT_FALSE_CORO(co_await enqueue_some_data());
    // drain all the batches
    ss::abort_source as;
    while (!_queue->empty()) {
        co_await _queue->fetch(as);
    }
    ASSERT_FALSE_CORO(_queue->full());
}

TEST_F_CORO(PartitionDataQueueTest, testConcurrentFetchEnqueue) {
    static constexpr auto test_time = 5s;
    auto deadline = ss::lowres_clock::now() + test_time;

    auto producer = [&](this auto) -> ss::future<> {
        while (ss::lowres_clock::now() < deadline) {
            co_await enqueue_some_data();
            co_await ss::sleep(
              std::chrono::milliseconds(random_generators::get_int(1, 5)));
        }
    };

    ss::abort_source as;
    auto fetcher = [&](this auto) -> ss::future<> {
        while (ss::lowres_clock::now() < deadline) {
            try {
                auto data = co_await _queue->fetch(as);
            } catch (const ss::abort_requested_exception&) {
                // ignore -- from resets/aborts
            }
            co_await ss::sleep(
              std::chrono::milliseconds(random_generators::get_int(1, 5)));
        }
    };

    auto resetter = [&](this auto) -> ss::future<> {
        while (ss::lowres_clock::now() < deadline) {
            _queue->reset(kafka::offset{});
            co_await ss::sleep(1s);
        }
    };
    auto abort_waiter = ss::sleep(test_time).then(
      [&as]() { as.request_abort(); });
    co_await ss::when_all_succeed(
      producer(), fetcher(), resetter(), std::move(abort_waiter));
}
