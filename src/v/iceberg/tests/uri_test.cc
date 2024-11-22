// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/provider.h"
#include "iceberg/uri.h"
#include "test_utils/test.h"

#include <variant>

using namespace iceberg;

template<auto Start, auto End, auto Inc, class F>
constexpr void constexpr_for(F&& f) {
    if constexpr (Start < End) {
        f(std::integral_constant<decltype(Start), Start>());
        constexpr_for<Start + Inc, End, Inc>(f);
    }
}

auto valid_paths = std::vector{
  std::filesystem::path("a"),
  std::filesystem::path("foo/bar/baz/manifest.avro"),
};

template<typename V>
void test_uri_conversion();

template<>
void test_uri_conversion<cloud_io::s3_compat_provider>() {
    auto bucket = cloud_storage_clients::bucket_name("testbucket1");
    auto other_bucket = cloud_storage_clients::bucket_name("testbucket2");

    auto other_converter = uri_converter(
      cloud_io::s3_compat_provider{"otherscheme"});

    auto s3_compat_provider_schemes = std::vector{"s3", "gcs"};
    for (auto scheme : s3_compat_provider_schemes) {
        uri_converter convertor(
          cloud_io::provider{cloud_io::s3_compat_provider{scheme}});

        for (const auto& path : valid_paths) {
            auto uri = convertor.to_uri(bucket, path);

            // Forward
            ASSERT_EQ(
              uri, fmt::format("{}://testbucket1/{}", scheme, path.native()));

            // Backward
            auto path_res = convertor.from_uri(bucket, uri);
            ASSERT_TRUE(path_res.has_value())
              << "Failed to get path from URI: " << uri;
            ASSERT_EQ(path_res.value(), path);

            // Should fail with different bucket.
            ASSERT_FALSE(convertor.from_uri(other_bucket, uri).has_value());

            // Should fail with other converter.
            ASSERT_FALSE(other_converter.from_uri(bucket, uri).has_value());
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
            path.native()));

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

TEST(IcebergUriTest, Test) { test_uri_conversion_all_providers(); }
