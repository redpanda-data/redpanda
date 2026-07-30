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

TEST(test_filters, test_default_include_no_filters) {
    EXPECT_TRUE(select_topic_default_include(::model::topic{"test-topic"}, {}));
}

TEST(test_filters, test_default_include_exclude_only) {
    chunked_vector<resource_name_filter_pattern> patterns{{
      .pattern_type = filter_pattern_type::literal,
      .filter = filter_type::exclude,
      .pattern = "secret",
    }};
    EXPECT_FALSE(
      select_topic_default_include(::model::topic{"secret"}, patterns));
    EXPECT_TRUE(
      select_topic_default_include(::model::topic{"other"}, patterns));
}

TEST(test_filters, test_default_include_prefix_pattern) {
    chunked_vector<resource_name_filter_pattern> patterns{{
      .pattern_type = filter_pattern_type::prefix,
      .filter = filter_type::include,
      .pattern = "app-",
    }};
    EXPECT_TRUE(
      select_topic_default_include(::model::topic{"app-1"}, patterns));
    EXPECT_FALSE(
      select_topic_default_include(::model::topic{"other"}, patterns));
}

TEST(test_filters, test_default_include_exclude_wins) {
    chunked_vector<resource_name_filter_pattern> patterns{
      {
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::include,
        .pattern = resource_name_filter_pattern::wildcard,
      },
      {
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::exclude,
        .pattern = "app-debug",
      }};
    EXPECT_TRUE(
      select_topic_default_include(::model::topic{"app-1"}, patterns));
    EXPECT_FALSE(
      select_topic_default_include(::model::topic{"app-debug"}, patterns));
}
} // namespace cluster_link::model::tests
