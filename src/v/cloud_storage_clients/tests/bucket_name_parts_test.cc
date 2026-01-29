/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/bucket_name_parts.h"

#include <gtest/gtest.h>

using cloud_storage_clients::bucket_name;
using cloud_storage_clients::parse_bucket_name;
using cloud_storage_clients::plain_bucket_name;

TEST(ParseBucketName, PlainBucketName) {
    auto result = parse_bucket_name(bucket_name{"my-bucket"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name, plain_bucket_name{"my-bucket"});
    EXPECT_TRUE(result->params.empty());
}

TEST(ParseBucketName, EmptyBucketNameRejected) {
    auto result = parse_bucket_name(bucket_name{""});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), "bucket: name cannot be empty");
}

TEST(ParseBucketName, WithParams) {
    // Tests: multiple params, percent-encoded value, unencoded URL value
    auto result = parse_bucket_name(
      bucket_name{"my-bucket?region=us-west-2"
                  "&encoded=http%3A%2F%2Flocalhost"
                  "&unencoded=http://localhost"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name, plain_bucket_name{"my-bucket"});
    ASSERT_EQ(result->params.size(), 3);
    EXPECT_EQ(result->params[0].first, "region");
    EXPECT_EQ(result->params[0].second, "us-west-2");
    EXPECT_EQ(result->params[1].first, "encoded");
    EXPECT_EQ(result->params[1].second, "http://localhost");
    EXPECT_EQ(result->params[2].first, "unencoded");
    EXPECT_EQ(result->params[2].second, "http://localhost");
}

TEST(ParseBucketName, TrailingQuestionMark) {
    auto result = parse_bucket_name(bucket_name{"my-bucket?"});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name, plain_bucket_name{"my-bucket"});
    EXPECT_TRUE(result->params.empty());
}

TEST(ParseBucketName, EmptyBucketWithParamsRejected) {
    auto result = parse_bucket_name(bucket_name{"?region=us-west-2"});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), "bucket: name cannot be empty");
}
