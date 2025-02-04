/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/provider.h"
#include "datalake/location.h"
#include "iceberg/uri.h"

#include <gtest/gtest.h>

namespace datalake {

struct testing_accessor {
    constexpr static auto ctor_key = scoped_location::ctor_key{};
};

TEST(scoped_location, roundtrip) {
    scoped_location loc(
      testing_accessor::ctor_key,
      iceberg::uri("s3://bucket/a/b/c"),
      std::filesystem::path("a/b/c"));

    ASSERT_EQ(loc.to_uri(), iceberg::uri("s3://bucket/a/b/c"));
    ASSERT_EQ(loc.to_key(), std::filesystem::path("a/b/c"));
}

TEST(scoped_location, append) {
    scoped_location loc(
      testing_accessor::ctor_key,
      iceberg::uri("s3://bucket/a/b/c"),
      std::filesystem::path("a/b/c"));

    // The path will be forced to be relative.
    for (auto path : {"d/e", "/d/e"}) {
        auto new_loc = loc.append_path(std::filesystem::path(path));
        ASSERT_EQ(new_loc.to_uri(), iceberg::uri("s3://bucket/a/b/c/d/e"));
        ASSERT_EQ(new_loc.to_key(), std::filesystem::path("a/b/c/d/e"));
    }

    // Append multiple paths.
    auto new_loc = loc.append_path("d").append_path("/e").append_path("f/g");
    ASSERT_EQ(new_loc.to_uri(), iceberg::uri("s3://bucket/a/b/c/d/e/f/g"));
    ASSERT_EQ(new_loc.to_key(), std::filesystem::path("a/b/c/d/e/f/g"));
}

TEST(scoped_location, replace) {
    scoped_location loc(
      testing_accessor::ctor_key,
      iceberg::uri("s3://bucket/a/b/c"),
      std::filesystem::path("a/b/c"));

    // Replace the path with a new one.
    ASSERT_EQ(
      loc.replace_path("a/b/c").to_uri(), iceberg::uri("s3://bucket/a/b/c"));
    ASSERT_EQ(
      loc.replace_path("a/b/c").to_key(), std::filesystem::path("a/b/c"));

    ASSERT_EQ(
      loc.replace_path("a/b/c/d/e/f").to_uri(),
      iceberg::uri("s3://bucket/a/b/c/d/e/f"));
    ASSERT_EQ(
      loc.replace_path("a/b/c/d/e/f").to_key(),
      std::filesystem::path("a/b/c/d/e/f"));

    // Replace the path with a new one that is not rooted at the same scope.
    for (auto path : {"bucket/a/b/c", "bucket/a/b/c/d/e", "e", "/e"}) {
        ASSERT_THROW(loc.replace_path(path), std::invalid_argument);
    }
}

TEST(location_provider, scoped_location) {
    location_provider provider(
      cloud_io::s3_compat_provider{"s3"},
      cloud_storage_clients::bucket_name("bucket"));

    {
        SCOPED_TRACE("Simple");
        auto loc = provider.make_scoped_location(
          iceberg::uri("s3://bucket/a/b/c"));
        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->to_uri(), iceberg::uri("s3://bucket/a/b/c"));
        ASSERT_EQ(loc->to_key(), std::filesystem::path("a/b/c"));
    }

    {
        SCOPED_TRACE("Trailing slash");
        auto loc = provider.make_scoped_location(
          iceberg::uri("s3://bucket/a/b/c/"));
        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->to_uri(), iceberg::uri("s3://bucket/a/b/c"));
        ASSERT_EQ(loc->to_key(), std::filesystem::path("a/b/c"));
    }

    {
        SCOPED_TRACE("Custom scheme. Not yet supported.");
        auto loc = provider.make_scoped_location(
          iceberg::uri("s3a://bucket/a/b/c"));
        ASSERT_FALSE(loc.has_value());
    }
}

} // namespace datalake
