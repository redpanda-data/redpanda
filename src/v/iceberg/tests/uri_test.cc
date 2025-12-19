/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/provider.h"
#include "iceberg/uri.h"

#include <gtest/gtest.h>

#include <variant>

namespace {
using namespace iceberg;

template<auto Start, auto End, auto Inc, class F>
constexpr void constexpr_for(F&& f) {
    if constexpr (Start < End) {
        f(std::integral_constant<decltype(Start), Start>());
        constexpr_for<Start + Inc, End, Inc>(std::forward<F>(f));
    }
}

constexpr auto valid_paths = std::to_array({"a", "foo/bar/baz/manifest.avro"});

template<typename V>
void test_uri_conversion();

template<>
void test_uri_conversion<cloud_io::s3_compat_provider>() {
    const std::vector<cloud_storage_clients::bucket_name> examples = {
      cloud_storage_clients::bucket_name{"testbucket1"},
      cloud_storage_clients::bucket_name{"test.bucket.name"}};

    const auto unrelated_bucket = cloud_storage_clients::bucket_name(
      "testbucket2");

    const auto other_converter = uri_converter(
      cloud_io::s3_compat_provider{"otherscheme"});

    auto s3_compat_provider_schemes = std::vector{"s3", "gcs"};
    for (auto scheme : s3_compat_provider_schemes) {
        uri_converter convertor(
          cloud_io::provider{cloud_io::s3_compat_provider{scheme}});

        for (const auto& b : examples) {
            for (const auto& path : valid_paths) {
                auto uri = convertor.to_uri(b, path);

                // Forward
                ASSERT_EQ(uri, fmt::format("{}://{}/{}", scheme, b, path));

                // Backward
                auto path_res = convertor.from_uri(b, uri);
                ASSERT_TRUE(path_res.has_value())
                  << "Failed to get path from URI: " << uri;
                ASSERT_EQ(path_res.value(), path);

                // Should fail with different bucket.
                ASSERT_FALSE(
                  convertor.from_uri(unrelated_bucket, uri).has_value());

                // Should fail with other converter.
                ASSERT_FALSE(other_converter.from_uri(b, uri).has_value());
            }
        }
    }
}

template<>
void test_uri_conversion<cloud_io::abs_provider>() {
    auto bucket = cloud_storage_clients::bucket_name("testbucket1");
    auto other_bucket = cloud_storage_clients::bucket_name("testbucket2");

    uri_converter convertor(cloud_io::abs_provider{"testaccount123"});
    uri_converter other_converter(cloud_io::abs_provider{"otheraccount123"});

    for (const auto& path : valid_paths) {
        auto uri = convertor.to_uri(bucket, path);

        // Forward
        ASSERT_EQ(
          uri,
          fmt::format(
            "abfss://testbucket1@testaccount123.dfs.core.windows.net/{}",
            path));

        // Backward
        auto path_res = convertor.from_uri(bucket, uri);
        ASSERT_TRUE(path_res.has_value())
          << "Failed to get path from URI: " << uri;
        ASSERT_EQ(path_res.value(), path);

        // Should fail to convert URI with different bucket name.
        ASSERT_FALSE(convertor.from_uri(other_bucket, uri).has_value());

        // Should fail to convert URI with different account name.
        ASSERT_FALSE(other_converter.from_uri(bucket, uri).has_value());
    }
}

constexpr void test_uri_conversion_all_providers() {
    constexpr size_t cnt = std::variant_size_v<cloud_io::provider>;

    constexpr_for<size_t(0), cnt, size_t(1)>([&](auto i) {
        test_uri_conversion<
          std::variant_alternative_t<i.value, cloud_io::provider>>();
    });
}

} // namespace

TEST(IcebergUriTest, Test) { test_uri_conversion_all_providers(); }
