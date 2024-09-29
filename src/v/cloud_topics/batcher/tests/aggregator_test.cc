/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/batcher/aggregator.h"
#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/batcher/write_request.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics = experimental::cloud_topics;

static ss::logger test_log("aggregator_test_log"); // NOLINT

cloud_topics::details::serialized_chunk get_random_serialized_chunk(
  int num_batches, int num_records_per_batch) { // NOLINT
    ss::circular_buffer<model::record_batch> batches;
    model::offset o{0};
    for (int ix_batch = 0; ix_batch < num_batches; ix_batch++) {
        auto batch = model::test::make_random_batch(
          o, num_records_per_batch, false);
        o = model::next_offset(batch.last_offset());
        batches.push_back(std::move(batch));
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    auto fut = cloud_topics::details::serialize_in_memory_record_batch_reader(
      std::move(reader));
    return fut.get();
}

TEST(AggregatorTest, SingleRequestAck) {
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    // The aggregator produces single L0 object
    cloud_topics::details::aggregator<ss::manual_clock> aggregator;

    aggregator.add(request);
    auto dest = aggregator.prepare();

    aggregator.ack();
    ASSERT_TRUE(fut.available());
    ASSERT_EQ(dest.size_bytes(), aggregator.size_bytes());
}

TEST(AggregatorTest, SingleRequestDtorWithStagedRequest) {
    // Check that if the write request is added to the aggregator it
    // will eventually be acknowledged even if the method is not invoked
    // explicitly.
    // In this case the request is dropped because the aggregator itself
    // is destroyed after the request is staged.
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::details::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(err.has_error());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithPreparedRequest) {
    // Check that if the write request is acknowledged with error if the
    // prepared request is never acknowledged and aggregator is destroyed.
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::details::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        std::ignore = aggregator.prepare();
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(err.has_error());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithLostRequestStaged) {
    // Checks situation when the aggregator is destroyed with staging write
    // requests but one write request is destroyed before the aggregator.
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::details::write_request<ss::manual_clock> tmp_request(
          model::controller_ntp,
          cloud_topics::details::batcher_req_index(1),
          std::move(chunk),
          timeout);
        cloud_topics::details::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        aggregator.add(tmp_request);
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(err.has_error());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithLostRequestPrepared) {
    // Checks situation when the aggregator is destroyed with outstanding write
    // requests but one write request is destroyed before the aggregator. So
    // basically, this mimics a situation when we uploaded L0 object that has
    // data from some produce request but the request is gone before it could be
    // acknowledged. The difference from previous test case is that 'prepare'
    // method is called.
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::details::write_request<ss::manual_clock> tmp_request(
          model::controller_ntp,
          cloud_topics::details::batcher_req_index(1),
          get_random_serialized_chunk(10, 10),
          timeout);
        cloud_topics::details::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        aggregator.add(tmp_request);
        std::ignore = aggregator.prepare();
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(err.has_error());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestWithLostRequestPrepared) {
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(
      model::controller_ntp,
      cloud_topics::details::batcher_req_index(0),
      std::move(chunk),
      timeout);
    auto fut = request.response.get_future();

    cloud_topics::details::aggregator<ss::manual_clock> aggregator;
    aggregator.add(request);
    {
        cloud_topics::details::write_request<ss::manual_clock> tmp_request(
          model::controller_ntp,
          cloud_topics::details::batcher_req_index(1),
          get_random_serialized_chunk(10, 10),
          timeout);
        aggregator.add(tmp_request);
    }

    auto dest = aggregator.prepare();

    aggregator.ack();
    ASSERT_TRUE(fut.available());
    ASSERT_LT(dest.size_bytes(), aggregator.size_bytes());
}
