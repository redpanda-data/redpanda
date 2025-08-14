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

#include "cluster_link/model/filter_utils.h"
#include "cluster_link/model/types.h"

#include <gtest/gtest.h>

namespace cluster_link::model::tests {
TEST(test_filters, test_no_filters) {
    EXPECT_FALSE(select_topic(::model::topic{"test-topic"}, {}));
}

TEST(test_filters, test_select_all) {
    chunked_vector<resource_name_filter_pattern> patterns{{
      .pattern_type = filter_pattern_type::literal,
      .filter = filter_type::include,
      .pattern = resource_name_filter_pattern::wildcard,
    }};

    EXPECT_TRUE(select_topic(::model::topic{"test-topic"}, patterns));
}

TEST(test_filters, test_literal_pattern) {
    chunked_vector<resource_name_filter_pattern> patterns{{
      .pattern_type = filter_pattern_type::literal,
      .filter = filter_type::include,
      .pattern = "test-topic",
    }};
    EXPECT_TRUE(select_topic(::model::topic{"test-topic"}, patterns));
    EXPECT_FALSE(select_topic(::model::topic{"other-topic"}, patterns));
}

TEST(test_filters, test_prefix_pattern) {
    chunked_vector<resource_name_filter_pattern> patterns{{
      .pattern_type = filter_pattern_type::prefix,
      .filter = filter_type::include,
      .pattern = "test-",
    }};
    EXPECT_TRUE(select_topic(::model::topic{"test-topic"}, patterns));
    EXPECT_FALSE(select_topic(::model::topic{"other-topic"}, patterns));
}

TEST(test_filters, test_exclude_pattern) {
    chunked_vector<resource_name_filter_pattern> patterns{
      {
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::exclude,
        .pattern = "test-topic",
      },
      {
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::include,
        .pattern = resource_name_filter_pattern::wildcard,
      }};
    EXPECT_FALSE(select_topic(::model::topic{"test-topic"}, patterns));
    EXPECT_TRUE(select_topic(::model::topic{"other-topic"}, patterns));
}
} // namespace cluster_link::model::tests
