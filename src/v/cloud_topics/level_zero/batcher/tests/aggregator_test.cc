/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/batcher/aggregator.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"

#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

static cloud_topics::cluster_epoch min_epoch{3840};

static ss::logger test_log("aggregator_test_log"); // NOLINT

auto make_oid(int epoch) {
    return cloud_topics::object_id::create(cloud_topics::cluster_epoch{epoch});
}

cloud_topics::l0::serialized_chunk get_random_serialized_chunk(
  int num_batches, int num_records_per_batch) { // NOLINT
    chunked_vector<model::record_batch> batches;
    model::offset o{0};
    for (int ix_batch = 0; ix_batch < num_batches; ix_batch++) {
        auto batch = model::test::make_random_batch(
          o, num_records_per_batch, false);
        o = model::next_offset(batch.last_offset());
        batches.push_back(std::move(batch));
    }
    auto fut = cloud_topics::l0::serialize_batches(std::move(batches));
    return fut.get();
}

TEST(AggregatorTest, SingleRequestAck) {
    auto timeout = ss::manual_clock::now() + 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto fut = request.response.get_future();

    // The aggregator produces single L0 object
    cloud_topics::l0::aggregator<ss::manual_clock> aggregator;

    aggregator.add(request);
    auto dest = aggregator.prepare(make_oid(0));

    aggregator.ack();
    ASSERT_TRUE(fut.available());
    ASSERT_EQ(dest.payload.size_bytes(), aggregator.size_bytes());
}

TEST(AggregatorTest, SingleRequestDtorWithStagedRequest) {
    // Check that if the write request is added to the aggregator it
    // will eventually be acknowledged even if the method is not invoked
    // explicitly.
    // In this case the request is dropped because the aggregator itself
    // is destroyed after the request is staged.
    auto timeout = ss::manual_clock::now() + 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(!err.has_value());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithPreparedRequest) {
    // Check that if the write request is acknowledged with error if the
    // prepared request is never acknowledged and aggregator is destroyed.
    auto timeout = ss::manual_clock::now() + 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        std::ignore = aggregator.prepare(make_oid(0));
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(!err.has_value());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithLostRequestStaged) {
    // Checks situation when the aggregator is destroyed with staging write
    // requests but one write request is destroyed before the aggregator.
    auto timeout = ss::manual_clock::now() + 10s;
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp,
      min_epoch,
      get_random_serialized_chunk(10, 10),
      timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        {
            // tmp_request is destroyed at the end of this block and aggregator
            // is destroyed outside
            cloud_topics::l0::write_request<ss::manual_clock> tmp_request(
              model::controller_ntp,
              min_epoch,
              get_random_serialized_chunk(10, 10),
              timeout);
            aggregator.add(tmp_request);
        }
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(!err.has_value());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestDtorWithLostRequestPrepared) {
    // Checks situation when the aggregator is destroyed with outstanding write
    // requests but one write request is destroyed before the aggregator. So
    // basically, this mimics a situation when we uploaded L0 object that has
    // data from some produce request but the request is gone before it could be
    // acknowledged. The difference from previous test case is that 'prepare'
    // method is called.
    auto timeout = ss::manual_clock::now() + 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto fut = request.response.get_future();

    {
        cloud_topics::l0::write_request<ss::manual_clock> tmp_request(
          model::controller_ntp,
          min_epoch,
          get_random_serialized_chunk(10, 10),
          timeout);
        cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
        aggregator.add(request);
        aggregator.add(tmp_request);
        std::ignore = aggregator.prepare(make_oid(0));
    }
    ASSERT_TRUE(fut.available());
    auto err = fut.get();
    ASSERT_TRUE(!err.has_value());
    ASSERT_TRUE(err.error() == cloud_topics::errc::timeout);
}

TEST(AggregatorTest, SingleRequestWithLostRequestPrepared) {
    auto timeout = ss::manual_clock::now() + 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::l0::write_request<ss::manual_clock> request(
      model::controller_ntp, min_epoch, std::move(chunk), timeout);
    auto fut = request.response.get_future();

    cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
    aggregator.add(request);
    {
        cloud_topics::l0::write_request<ss::manual_clock> tmp_request(
          model::controller_ntp,
          min_epoch,
          get_random_serialized_chunk(10, 10),
          timeout);
        aggregator.add(tmp_request);
    }

    auto dest = aggregator.prepare(make_oid(0));

    aggregator.ack();
    ASSERT_TRUE(fut.available());
    ASSERT_LT(dest.payload.size_bytes(), aggregator.size_bytes());
}

TEST(AggregatorTest, MinEpoch) {
    auto timeout = ss::manual_clock::now() + 10s;

    std::vector<
      std::unique_ptr<cloud_topics::l0::write_request<ss::manual_clock>>>
      requests;
    auto make_request = [&](int epoch) {
        auto chunk = get_random_serialized_chunk(10, 10);
        requests.push_back(
          std::make_unique<cloud_topics::l0::write_request<ss::manual_clock>>(
            model::controller_ntp,
            cloud_topics::cluster_epoch(epoch),
            std::move(chunk),
            timeout));
    };

    make_request(5);
    make_request(10);
    make_request(7);

    cloud_topics::l0::aggregator<ss::manual_clock> aggregator;
    for (auto& req : requests) {
        aggregator.add(*req);
    }

    ASSERT_EQ(aggregator.highest_topic_start_epoch()(), 10);
}
