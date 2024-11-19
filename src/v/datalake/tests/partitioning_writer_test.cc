/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/partitioning_writer.h"
#include "datalake/table_definition.h"
#include "datalake/tests/test_data_writer.h"
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
        type.fields.emplace_back(nested_field::create(
          type.fields.size() + 1,
          fmt::format("foo-{}", i),
          field_required::no,
          int_type{}));
    }
    return type;
}
} // namespace

class PartitioningWriterExtraColumnsTest
  : public ::testing::TestWithParam<size_t> {};

TEST_P(PartitioningWriterExtraColumnsTest, TestSchemaHappyPath) {
    const auto extra_columns = GetParam();
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    auto field = field_type{default_type_with_columns(extra_columns)};
    auto& default_type = std::get<struct_type>(field);
    partitioning_writer writer(*writer_factory, default_type.copy());

    // Create a bunch of records spread over multiple hours.
    static constexpr auto ms_per_hr = 3600 * 1000;
    static constexpr auto num_hrs = 10;
    static constexpr auto records_per_hr = 5;
    const auto start_time = model::timestamp::now();
    chunked_vector<struct_value> source_vals;
    for (int h = 0; h < num_hrs; h++) {
        for (int i = 0; i < records_per_hr; i++) {
            source_vals.emplace_back(val_with_timestamp(
              field, model::timestamp{start_time.value() + h * ms_per_hr + i}));
        }
    }
    // Give the data to the partitioning writer.
    for (auto& v : source_vals) {
        auto err = writer.add_data(std::move(v), /*approx_size=*/0).get();
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
        total_records += f.row_count;
        min_hr = std::min(f.hour, min_hr);
        max_hr = std::max(f.hour, max_hr);
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
    partitioning_writer writer(*writer_factory, default_type.copy());
    auto err = writer
                 .add_data(
                   val_with_timestamp(field, model::timestamp::now()),
                   /*approx_size=*/0)
                 .get();
    EXPECT_EQ(err, writer_error::parquet_conversion_error);
}

TEST(PartitioningWriterTest, TestUnexpectedSchema) {
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    partitioning_writer writer(*writer_factory, default_type_with_columns(0));
    auto unexpected_field_type = test_nested_schema_type();
    auto err = writer
                 .add_data(
                   val_with_timestamp(
                     unexpected_field_type, model::timestamp::now()),
                   /*approx_size=*/0)
                 .get();
    EXPECT_EQ(err, writer_error::parquet_conversion_error);
}
