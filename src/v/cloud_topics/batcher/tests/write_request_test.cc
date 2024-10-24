/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/write_request.h"
#include "model/namespace.h"

#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

namespace cloud_topics = experimental::cloud_topics;

TEST(WriteRequestTest, Expiration) {
    cloud_topics::details::write_request<ss::manual_clock> req(
      model::kvstore_ntp(ss::shard_id(0)),
      cloud_topics::details::batcher_req_index(0),
      {},
      std::chrono::milliseconds(100));
    ASSERT_FALSE(req.has_expired());
    ss::manual_clock::advance(std::chrono::milliseconds(10));
    ASSERT_FALSE(req.has_expired());
    ss::manual_clock::advance(std::chrono::milliseconds(100));
    ASSERT_TRUE(req.has_expired());
}
