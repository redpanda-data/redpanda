/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "model/namespace.h"

#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

static cloud_topics::cluster_epoch min_epoch{3840};
static model::topic_id test_topic_id = model::topic_id::create();

TEST(WriteRequestTest, Expiration) {
    cloud_topics::l0::write_request<ss::manual_clock> req(
      model::kvstore_ntp(ss::shard_id(0)),
      test_topic_id,
      min_epoch,
      {},
      ss::manual_clock::now() + std::chrono::milliseconds(100));
    ASSERT_FALSE(req.has_expired());
    ss::manual_clock::advance(std::chrono::milliseconds(10));
    ASSERT_FALSE(req.has_expired());
    ss::manual_clock::advance(std::chrono::milliseconds(100));
    ASSERT_TRUE(req.has_expired());
}
