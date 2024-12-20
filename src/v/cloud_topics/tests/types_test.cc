/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/types.h"

#include <gtest/gtest.h>

TEST(DlStmKey, Formatting) {
    ASSERT_EQ(
      fmt::format("{}", experimental::cloud_topics::dl_stm_key::push_overlay),
      "push_overlay");

    ASSERT_EQ(
      fmt::format("{}", experimental::cloud_topics::dl_stm_key(999)),
      "unknown dl_stm_key(999)");
}
