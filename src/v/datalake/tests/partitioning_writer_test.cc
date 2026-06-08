/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/bytes.h"
#include "datalake/partitioning_writer.h"
#include "datalake/table_definition.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

#include <limits>

using namespace datalake;
using namespace iceberg;

namespace {
struct_value
val_with_timestamp(const field_type& type, model::timestamp timestamp_ms) {
    static constexpr auto micros_per_ms = 1000;
    auto timestamp_us = timestamp_ms.value() * micros_per_ms;
    auto val = tests::make_value({.forced_num_val = timestamp_us}, type);
    return std::move(*std::get<std::unique_ptr<struct_value>>(val));
}
iceberg::struct_type default_type_with_columns(size_t extra_columns) {
    auto type = schemaless_struct_type();
    for (size_t i = 0; i < extra_columns; ++i) {
        type.fields.emplace_back(
          nested_field::create(
            type.fields.size() + 1,
            fmt::format("foo-{}", i),
            field_required::no,
            int_type{}));
    }
    return type;
}

static constexpr auto schema_id = iceberg::schema::id_t{0};

static constexpr auto ms_per_hr = 3600 * 1000;
static constexpr int64_t ms_per_day = int64_t{24} * ms_per_hr;

// Create a bunch of records spread over multiple hours.
chunked_vector<struct_value> generate_values(
  const field_type& schema_field, int num_hrs, int records_per_hr) {
    const auto start_time = model::timestamp::now();
    chunked_vector<struct_value> vals;
    for (int h = 0; h < num_hrs; h++) {
        for (int i = 0; i < records_per_hr; i++) {
            vals.emplace_back(val_with_timestamp(
              schema_field,
              model::timestamp{start_time.value() + h * ms_per_hr + i}));
        }
    }
    return vals;
}

// Create records spread across multiple UTC days, anchored to a deterministic
// midnight so the test is not sensitive to wall-clock time of day.
chunked_vector<struct_value> generate_values_over_days(
  const field_type& schema_field, int num_days, int records_per_day) {
    const int64_t midnight = (model::timestamp::now().value() / ms_per_day)
                             * ms_per_day;
    chunked_vector<struct_value> vals;
    for (int d = 0; d < num_days; d++) {
        for (int i = 0; i < records_per_day; i++) {
            vals.emplace_back(val_with_timestamp(
              schema_field, model::timestamp{midnight + d * ms_per_day + i}));
        }
    }
    return vals;
}

ss::abort_source as;

} // namespace

class PartitioningWriterExtraColumnsTest
  : public ::testing::TestWithParam<size_t> {};

TEST_P(PartitioningWriterExtraColumnsTest, TestSchemaHappyPath) {
    const auto extra_columns = GetParam();
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    auto field = field_type{default_type_with_columns(extra_columns)};
    auto& default_type = std::get<struct_type>(field);
    auto pspec = partition_spec::resolve(hour_partition_spec(), default_type);
    ASSERT_TRUE(pspec.has_value());
    partitioning_writer writer(
      *writer_factory,
      schema_id,
      default_type.copy(),
      pspec.value().copy(),
      {});

    static constexpr int num_hrs = 5;
    static constexpr int records_per_hr = 5;
    auto source_vals = generate_values(field, num_hrs, records_per_hr);

    // Give the data to the partitioning writer.
    for (auto& v : source_vals) {
        auto err = writer.add_data(std::move(v), /*approx_size=*/0, as).get();
        EXPECT_EQ(err, writer_error::ok);
    }

    // The resulting files should match the number of hours the records were
    // spread across.
    auto res = std::move(writer).finish().get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& files = res.value();
    ASSERT_EQ(num_hrs, files.size());
    int min_hr = std::numeric_limits<int>::max();
    int max_hr = std::numeric_limits<int>::min();
    size_t total_records = 0;
    for (const auto& f : files) {
        total_records += f.local_file.row_count;
        int hour = get_hour(f.partition_key);
        min_hr = std::min(hour, min_hr);
        max_hr = std::max(hour, max_hr);
    }
    EXPECT_EQ(num_hrs - 1, max_hr - min_hr);
    EXPECT_EQ(total_records, records_per_hr * num_hrs);
}

INSTANTIATE_TEST_SUITE_P(
  ExtraColumns, PartitioningWriterExtraColumnsTest, ::testing::Values(0, 10));

TEST(PartitioningWriterTest, TestWriterError) {
    // Writer factory with errors!
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      true);
    auto field = field_type{default_type_with_columns(0)};
    auto& default_type = std::get<struct_type>(field);
    auto pspec = partition_spec::resolve(hour_partition_spec(), default_type);
    ASSERT_TRUE(pspec.has_value());
    partitioning_writer writer(
      *writer_factory,
      schema_id,
      default_type.copy(),
      pspec.value().copy(),
      {});
    auto err = writer
                 .add_data(
                   val_with_timestamp(field, model::timestamp::now()),
                   /*approx_size=*/0,
                   as)
                 .get();
    EXPECT_EQ(err, writer_error::parquet_conversion_error);
}

TEST(PartitioningWriterTest, TestUnexpectedSchema) {
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    auto schema_type = default_type_with_columns(0);
    auto pspec = partition_spec::resolve(hour_partition_spec(), schema_type);
    ASSERT_TRUE(pspec.has_value());
    partitioning_writer writer(
      *writer_factory, schema_id, schema_type.copy(), pspec.value().copy(), {});
    auto unexpected_field_type = test_nested_schema_type();
    auto err = writer
                 .add_data(
                   val_with_timestamp(
                     unexpected_field_type, model::timestamp::now()),
                   /*approx_size=*/0,
                   as)
                 .get();
    EXPECT_EQ(err, writer_error::parquet_conversion_error);
}

TEST(PartitioningWriterTest, TestEmptyKey) {
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    auto field = field_type{default_type_with_columns(0)};
    auto& default_type = std::get<struct_type>(field);
    auto spec_id = partition_spec::id_t{123};
    partition_spec empty_spec{.spec_id = spec_id};
    partitioning_writer writer(
      *writer_factory, schema_id, default_type.copy(), empty_spec.copy(), {});

    static constexpr auto num_hrs = 10;
    static constexpr auto records_per_hr = 5;
    auto source_vals = generate_values(field, num_hrs, records_per_hr);

    // Give the data to the partitioning writer.
    for (auto& v : source_vals) {
        auto err = writer.add_data(std::move(v), /*approx_size=*/0, as).get();
        EXPECT_EQ(err, writer_error::ok);
    }

    // There should be only one file corresponding to the single partition.
    auto res = std::move(writer).finish().get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& files = res.value();
    ASSERT_EQ(files.size(), 1);

    const auto& file = files[0];
    ASSERT_EQ(file.partition_spec_id, spec_id);
    ASSERT_EQ(file.partition_key.val->fields.size(), 0);
}

TEST(PartitioningWriterTest, TestDayTransform) {
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    auto field = field_type{default_type_with_columns(0)};
    auto& default_type = std::get<struct_type>(field);
    auto pspec = partition_spec::resolve(day_partition_spec(), default_type);
    ASSERT_TRUE(pspec.has_value());
    partitioning_writer writer(
      *writer_factory,
      schema_id,
      default_type.copy(),
      pspec.value().copy(),
      {});

    static constexpr int num_days = 4;
    static constexpr int records_per_day = 5;
    auto source_vals = generate_values_over_days(
      field, num_days, records_per_day);

    for (auto& v : source_vals) {
        auto err = writer.add_data(std::move(v), /*approx_size=*/0, as).get();
        EXPECT_EQ(err, writer_error::ok);
    }

    auto res = std::move(writer).finish().get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& files = res.value();
    ASSERT_EQ(num_days, files.size());

    int32_t min_day = std::numeric_limits<int32_t>::max();
    int32_t max_day = std::numeric_limits<int32_t>::min();
    size_t total_records = 0;
    for (const auto& f : files) {
        total_records += f.local_file.row_count;
        ASSERT_EQ(f.partition_key.val->fields.size(), 1);
        const auto& key_field = f.partition_key.val->fields[0];
        ASSERT_TRUE(key_field.has_value());
        const auto& prim = std::get<primitive_value>(key_field.value());
        // Per the Iceberg spec, the day transform produces a date value, not
        // an int value.
        ASSERT_TRUE(std::holds_alternative<date_value>(prim));
        int32_t day = get_day(f.partition_key);
        min_day = std::min(day, min_day);
        max_day = std::max(day, max_day);
    }
    EXPECT_EQ(num_days - 1, max_day - min_day);
    EXPECT_EQ(total_records, records_per_day * num_days);
}

TEST(PartitioningWriterTest, TestCompositeKey) {
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);

    auto schema_type = default_type_with_columns(5);
    schema_type.fields.push_back(
      nested_field::create(
        schema_type.fields.size() + 1,
        "foo_str",
        field_required::no,
        string_type{}));

    auto schema_field = field_type{schema_type.copy()};

    unresolved_partition_spec unresolved_spec = hour_partition_spec();
    unresolved_spec.fields.push_back(
      unresolved_partition_spec::field{
        .source_name = {"foo_str"},
        .transform = identity_transform{},
        .name = "field2",
      });
    auto spec = partition_spec::resolve(unresolved_spec, schema_type);
    ASSERT_TRUE(spec.has_value());

    partitioning_writer writer(
      *writer_factory, schema_id, schema_type.copy(), spec.value().copy(), {});

    static constexpr auto num_hrs = 10;
    static constexpr auto records_per_hr = 5;
    auto source_vals = generate_values(schema_field, num_hrs, records_per_hr);

    // For each record set a value for the string field to aaa/bbb/null.
    // rows in each hour will get all three values.
    for (size_t i = 0; i < source_vals.size(); ++i) {
        auto& string_field = source_vals[i].fields.back();
        switch (i % 3) {
        case 0:
            string_field = string_value{iobuf::from("aaa")};
            break;
        case 1:
            string_field = string_value{iobuf::from("bbb")};
            break;
        default:
            string_field = std::nullopt;
            break;
        }
    }

    // Give the data to the partitioning writer.
    for (auto& v : source_vals) {
        auto err = writer.add_data(std::move(v), /*approx_size=*/0, as).get();
        EXPECT_EQ(err, writer_error::ok);
    }

    auto res = std::move(writer).finish().get();
    ASSERT_FALSE(res.has_error()) << res.error();

    const auto& files = res.value();
    ASSERT_EQ(files.size(), 3 * num_hrs);

    // check that we've got a file for each possible partition key value.
    std::map<std::optional<bytes>, std::vector<int>> string2hours;
    for (const auto& file : files) {
        ASSERT_EQ(file.partition_spec_id, spec.value().spec_id);
        const auto& key_fields = file.partition_key.val->fields;
        ASSERT_EQ(key_fields.size(), 2);
        int hour = std::get<int_value>(
                     std::get<primitive_value>(key_fields[0].value()))
                     .val;
        std::optional<bytes> string;
        if (key_fields[1]) {
            string = iobuf_to_bytes(
              std::get<string_value>(
                std::get<primitive_value>(key_fields[1].value()))
                .val);
        }
        string2hours[string].push_back(hour);
    }

    ASSERT_TRUE(string2hours.contains(std::nullopt));
    ASSERT_EQ(string2hours[std::nullopt].size(), num_hrs);
    ASSERT_TRUE(string2hours.contains(bytes::from_string("aaa")));
    ASSERT_EQ(string2hours[bytes::from_string("aaa")].size(), num_hrs);
    ASSERT_TRUE(string2hours.contains(bytes::from_string("bbb")));
    ASSERT_EQ(string2hours[bytes::from_string("bbb")].size(), num_hrs);
}
