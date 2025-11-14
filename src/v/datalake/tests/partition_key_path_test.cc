/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/partition_key_path.h"
#include "datalake/tests/test_data.h"

#include <gmock/gmock.h>

using namespace datalake;
using namespace std::chrono_literals;
struct test_context {
    iceberg::partition_spec spec;
    iceberg::partition_key key;
};

test_context make_identity_partitions_spec() {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;

    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_bool"},
        .transform = iceberg::identity_transform{},
        .name = "bool_partition",
      });

    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_int"},
        .transform = iceberg::identity_transform{},
        .name = "int_partition",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_long"},
        .transform = iceberg::identity_transform{},
        .name = "long_test_partition",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_float"},
        .transform = iceberg::identity_transform{},
        .name = "fl_partition",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_double"},
        .transform = iceberg::identity_transform{},
        .name = "d_partition",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_decimal"},
        .transform = iceberg::identity_transform{},
        .name = "decimal_partition",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_date"},
        .transform = iceberg::identity_transform{},
        .name = "date_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_time"},
        .transform = iceberg::identity_transform{},
        .name = "time_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamptz"},
        .transform = iceberg::identity_transform{},
        .name = "timestamptz_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_string"},
        .transform = iceberg::identity_transform{},
        .name = "string_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_uuid"},
        .transform = iceberg::identity_transform{},
        .name = "uuid_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_fixed"},
        .transform = iceberg::identity_transform{},
        .name = "fixed_identity",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_binary"},
        .transform = iceberg::identity_transform{},
        .name = "binary_identity",
      });
    auto spec = iceberg::partition_spec::resolve(u_spec, schema);
    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();

    key.val->fields.push_back(iceberg::boolean_value{.val = true});
    key.val->fields.push_back(iceberg::int_value{.val = 128});
    key.val->fields.push_back(iceberg::long_value{.val = 4096});
    key.val->fields.push_back(iceberg::float_value{.val = 3.1415});
    key.val->fields.push_back(iceberg::double_value{.val = 2.7182});
    key.val->fields.push_back(
      iceberg::decimal_value{.val = absl::int128{1231123}});
    key.val->fields.push_back(iceberg::date_value{.val = 20140});
    key.val->fields.push_back(
      iceberg::time_value{
        .val = (std::chrono::hours(14) + 43min + 15s + 167ms) / 1us});
    key.val->fields.push_back(
      iceberg::timestamp_value{.val = 1740143929000000});
    key.val->fields.push_back(
      iceberg::timestamptz_value{.val = 1740143929000000});
    key.val->fields.push_back(
      iceberg::string_value{.val = iobuf::from("test_string_value")});
    key.val->fields.push_back(
      iceberg::uuid_value{
        .val = uuid_t::from_string("f47ac10b-58cc-4372-a567-0e02b2c3d479")});
    key.val->fields.push_back(
      iceberg::fixed_value{.val = iobuf::from("Hello world")});
    key.val->fields.push_back(
      iceberg::binary_value{.val = iobuf::from("PandasAreCuties")});

    return test_context{.spec = std::move(spec.value()), .key = std::move(key)};
}
/**
 * Test validating conversion of PartitionSpec containing only identity
 * transforms, this is special case as the identity transform is the only one
 * preserving the type of the field that it is calculated from.
 */
TEST(PartitionKeyPathConversionTest, TestIdentityTransform) {
    auto [spec, key] = make_identity_partitions_spec();

    auto res = partition_key_to_path(spec, key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(
      res.value()().string(),
      "bool_partition=true/"
      "int_partition=128/"
      "long_test_partition=4096/"
      "fl_partition=3.1415/"
      "d_partition=2.7182/"
      "decimal_partition=1231123/"
      "date_identity=2025-02-21/"
      "time_identity=14%3A43%3A15.167/"
      "timestamp_identity=2025-02-21T13%3A18%3A49Z/"
      "timestamptz_identity=2025-02-21T13%3A18%3A49%2B0000/"
      "string_identity=test_string_value/"
      "uuid_identity=f47ac10b-58cc-4372-a567-0e02b2c3d479/"
      "fixed_identity=SGVsbG8gd29ybGQ%3D/"
      "binary_identity=UGFuZGFzQXJlQ3V0aWVz");
}

TEST(PartitionKeyPathConversionTest, TestTimestampTransform) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_no_ms",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_ms",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_us",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamptz"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_tz_no_ms",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamptz"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_tz_ms",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamptz"},
        .transform = iceberg::identity_transform{},
        .name = "timestamp_tz_us",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_time"},
        .transform = iceberg::identity_transform{},
        .name = "time_s",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_time"},
        .transform = iceberg::identity_transform{},
        .name = "time_ms",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_time"},
        .transform = iceberg::identity_transform{},
        .name = "time_us",
      });

    auto spec = iceberg::partition_spec::resolve(u_spec, schema);

    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    // 10-02-2025 10:37:13
    key.val->fields.push_back(
      iceberg::timestamp_value{.val = 1739183833000000});
    // 10-02-2025 10:37:13.321
    key.val->fields.push_back(
      iceberg::timestamp_value{.val = 1739183833321000});
    // 10-02-2025 10:37:13.321123
    key.val->fields.push_back(
      iceberg::timestamp_value{.val = 1739183833321123});

    // 10-02-2025 10:37:13
    key.val->fields.push_back(
      iceberg::timestamptz_value{.val = 1739183833000000});
    // 10-02-2025 10:37:13.321
    key.val->fields.push_back(
      iceberg::timestamptz_value{.val = 1739183833321000});
    // 10-02-2025 10:37:13.321123
    key.val->fields.push_back(
      iceberg::timestamptz_value{.val = 1739183833321123});

    key.val->fields.push_back(
      iceberg::time_value{.val = (std::chrono::hours(11) + 11min + 11s) / 1us});
    key.val->fields.push_back(
      iceberg::time_value{
        .val = (std::chrono::hours(11) + 11min + 11s + 456ms) / 1us});
    key.val->fields.push_back(
      iceberg::time_value{
        .val = (std::chrono::hours(11) + 11min + 11s + 789us) / 1us});

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(
      res.value()().string(),
      "timestamp_no_ms=2025-02-10T10%3A37%3A13Z/"
      "timestamp_ms=2025-02-10T10%3A37%3A13.321Z/"
      "timestamp_us=2025-02-10T10%3A37%3A13.321123Z/"
      "timestamp_tz_no_ms=2025-02-10T10%3A37%3A13%2B0000/"
      "timestamp_tz_ms=2025-02-10T10%3A37%3A13.321%2B0000/"
      "timestamp_tz_us=2025-02-10T10%3A37%3A13.321123%2B0000/"
      "time_s=11%3A11%3A11/"
      "time_ms=11%3A11%3A11.456/"
      "time_us=11%3A11%3A11.000789"

    );
}

TEST(PartitionKeyPathConversionTest, TimeTransformsTest) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::year_transform{},
        .name = "year_transform",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::month_transform{},
        .name = "month_transform",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::day_transform{},
        .name = "day_transform",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_timestamp"},
        .transform = iceberg::hour_transform{},
        .name = "hour_transform",
      });
    auto spec = iceberg::partition_spec::resolve(u_spec, schema);

    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    key.val->fields.push_back(iceberg::int_value{.val = 2025 - 1969});
    key.val->fields.push_back(
      iceberg::int_value{.val = (2025 - 1970) * 12 + 5});

    key.val->fields.push_back(
      iceberg::int_value{
        .val = std::chrono::floor<std::chrono::days>(
                 std::chrono::system_clock::time_point(
                   std::chrono::seconds(1740396135))
                   .time_since_epoch())
                 .count()});
    key.val->fields.push_back(
      iceberg::int_value{
        .val = std::chrono::floor<std::chrono::hours>(
                 std::chrono::system_clock::time_point(
                   std::chrono::seconds(1740396135))
                   .time_since_epoch())
                 .count()});

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(
      res.value()().string(),
      "year_transform=2025/"
      "month_transform=2025-06/"
      "day_transform=2025-02-24/"
      "hour_transform=2025-02-24-11");
}

TEST(PartitionKeyPathConversionTest, VoidTransformTest) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_int"},
        .transform = iceberg::void_transform{},
        .name = "void_transform",
      });
    auto spec = iceberg::partition_spec::resolve(u_spec, schema);
    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    key.val->fields.push_back(iceberg::value{});

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(res.value()().string(), "void_transform=null");
}

TEST(PartitionKeyPathConversionTest, BucketTransformTest) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_int"},
        .transform = iceberg::bucket_transform{},
        .name = "bucket_transform",
      });
    auto spec = iceberg::partition_spec::resolve(u_spec, schema);
    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    key.val->fields.push_back(iceberg::int_value{.val = 32});

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(res.value()().string(), "bucket_transform=32");
}

TEST(PartitionKeyPathConversionTest, TestElementSizeLimiting) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_string"},
        .transform = iceberg::identity_transform{},
        .name = "identity_string",
      });
    u_spec.fields.push_back(
      iceberg::unresolved_partition_spec::field{
        .source_name = {"test_binary"},
        .transform = iceberg::identity_transform{},
        .name = "identity_binary",
      });

    auto spec = iceberg::partition_spec::resolve(u_spec, schema);
    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    key.val->fields.push_back(
      iceberg::string_value{
        .val = iobuf::from(
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
          "Pellentesque "
          "ipsum magna, pellentesque quis nisl eu, congue aliquam id.")});
    key.val->fields.push_back(
      iceberg::binary_value{
        .val = iobuf::from(
          "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
          "Pellentesque "
          "ipsum magna, pellentesque quis nisl eu, congue aliquam id.")});

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(
      res.value()().string(),
      "identity_string=Lorem%20ipsum%20dolor%20sit%20amet%2C%20consectetur%"
      "20adipiscing%20elit.%20Pellent/"
      "identity_binary="
      "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxp"
      "dC4gUGVsbGVudA%3D%3D");
}

TEST(PartitionKeyPathConversionTest, TestPathSizeLimitting) {
    auto schema = test_schema(iceberg::field_required::yes);

    iceberg::unresolved_partition_spec u_spec;
    // create lots of partition keys
    for (size_t i = 0; i < 64; ++i) {
        u_spec.fields.push_back(
          iceberg::unresolved_partition_spec::field{
            .source_name = {"test_int"},
            .transform = iceberg::identity_transform{},
            .name = fmt::format("identity_int_{}", i),
          });
    }

    auto spec = iceberg::partition_spec::resolve(u_spec, schema);
    iceberg::partition_key key;
    key.val = std::make_unique<iceberg::struct_value>();
    for (int i = 0; i < 64; ++i) {
        key.val->fields.push_back(iceberg::int_value{.val = i});
    }

    auto res = partition_key_to_path(spec.value(), key);

    ASSERT_FALSE(res.has_error());
    // make sure the path is truncated
    const auto path = res.value()().string();
    ASSERT_LE(path.size(), 512);
    // check the path ends wih the full segment
    ASSERT_THAT(path, ::testing::EndsWith("identity_int_27=27"));
}
