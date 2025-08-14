/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/object_utils.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#define UUID_REGEX                                                             \
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

TEST(ObjectPathFactory, LevelOnePathFormat) {
    auto path
      = experimental::cloud_topics::l1::object_path_factory::level_one_path(
        experimental::cloud_topics::l1::create_object_id());
    EXPECT_THAT(
      path().string(), ::testing::MatchesRegex("^l1_v0_" UUID_REGEX "$"));
}
