// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/partition.h"
#include "iceberg/partition_key.h"
#include "iceberg/struct_accessor.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/transform.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

using namespace iceberg;

field_type nested_type_with_timestamp(
  int field_id, field_required required = field_required::yes) {
    struct_type type;
    type.fields.emplace_back(
      nested_field::create(field_id, "ts", required, timestamp_type{}));
    auto nested_type = test_nested_schema_type();
    for (auto& f : std::get<struct_type>(nested_type).fields) {
        type.fields.emplace_back(std::move(f));
    }
    return type;
}

partition_spec make_ts_partition_spec(int32_t source_field_id) {
    partition_spec spec;
    spec.spec_id = partition_spec::id_t{0};
    spec.fields.emplace_back(partition_field{
      .source_id = nested_field::id_t{source_field_id},
      .field_id = partition_field::id_t{1000},
      .name = "hour",
      .transform = hour_transform{},
    });
    return spec;
}

struct_value
val_with_timestamp(const field_type& type, model::timestamp timestamp_ms) {
    static constexpr auto micros_per_ms = 1000;
    auto timestamp_us = timestamp_ms.value() * micros_per_ms;
    auto val = tests::make_value({.forced_num_val = timestamp_us}, type);
    return std::move(*std::get<std::unique_ptr<struct_value>>(val));
}

partition_key make_partition_key(
  const struct_value& v, const struct_type& t, int source_field_id) {
    const auto accessors = struct_accessor::from_struct_type(t);
    auto partition_spec = make_ts_partition_spec(source_field_id);
    return partition_key::create(v, accessors, partition_spec);
}

TEST(PartitionKeyTest, TestHourlyGrouping) {
    const auto ts_field_id = 0;
    auto schema_type = nested_type_with_timestamp(ts_field_id);
    auto partition_spec = make_ts_partition_spec(ts_field_id);
    const auto accessors = struct_accessor::from_struct_type(
      std::get<struct_type>(schema_type));

    chunked_vector<struct_value> source_vals;
    static constexpr auto ms_per_hr = 3600 * 1000;
    static constexpr auto num_hrs = 10;
    static constexpr auto records_per_hr = 5;
    const auto start_time = model::timestamp::now();
    for (int h = 0; h < num_hrs; h++) {
        for (int i = 0; i < records_per_hr; i++) {
            // Insert `records_per_hr` records in each hour.
            source_vals.emplace_back(val_with_timestamp(
              schema_type,
              model::timestamp{start_time.value() + h * ms_per_hr + i}));
        }
    }

    chunked_hash_map<partition_key, chunked_vector<struct_value>> vals_by_pk;
    for (auto& v : source_vals) {
        auto pk = partition_key::create(v, accessors, partition_spec);
        auto [iter, _] = vals_by_pk.emplace(
          std::move(pk), chunked_vector<struct_value>{});
        iter->second.emplace_back(std::move(v));
    }
    size_t num_vals = 0;
    for (const auto& [pk, vs] : vals_by_pk) {
        num_vals += vs.size();
    }
    ASSERT_EQ(vals_by_pk.size(), 10);
    ASSERT_EQ(num_vals, num_hrs * records_per_hr);
}

TEST(PartitionKeyTest, TestNullValuesKey) {
    const auto ts_field_id = 0;
    auto schema_type = nested_type_with_timestamp(ts_field_id);
    struct_value v;
    for (size_t i = 0; i < std::get<struct_type>(schema_type).fields.size();
         i++) {
        v.fields.emplace_back(std::nullopt);
    }
    const auto accessors = struct_accessor::from_struct_type(
      std::get<struct_type>(schema_type));

    auto partition_spec = make_ts_partition_spec(ts_field_id);
    auto pk = partition_key::create(v, accessors, partition_spec);
    ASSERT_EQ(1, pk.val->fields.size());
    ASSERT_FALSE(pk.val->fields[0].has_value());
}

TEST(PartitionKeyTest, TestBogusPartitionSpec) {
    const auto ts_field_id = 0;
    auto schema_type = nested_type_with_timestamp(ts_field_id);
    struct_value v;
    for (size_t i = 0; i < std::get<struct_type>(schema_type).fields.size();
         i++) {
        v.fields.emplace_back(std::nullopt);
    }
    const auto accessors = struct_accessor::from_struct_type(
      std::get<struct_type>(schema_type));

    auto partition_spec = make_ts_partition_spec(ts_field_id + 1000);
    ASSERT_THROW(
      partition_key::create(v, accessors, partition_spec),
      std::invalid_argument);
}

TEST(PartitionKeyTest, TestCopyPartitionKey) {
    const auto ts_field_id = 0;
    auto schema_type = nested_type_with_timestamp(0, field_required::no);
    {
        // Construct a partition key that has empty values and ensure it gets
        // copied.
        auto v = tests::make_value({.null_pct = 100}, schema_type);
        auto pk = make_partition_key(
          *std::get<std::unique_ptr<struct_value>>(v),
          std::get<struct_type>(schema_type),
          ts_field_id);
        auto pk_copy = pk.copy();
        ASSERT_EQ(pk_copy, pk);
        for (const auto& f : pk_copy.val->fields) {
            ASSERT_FALSE(f.has_value()) << f.value();
        }
    }
    {
        // Construct a partition key that has non-empty values and ensure it
        // gets copied.
        auto v = tests::make_value({.null_pct = 0}, schema_type);
        auto pk = make_partition_key(
          *std::get<std::unique_ptr<struct_value>>(v),
          std::get<struct_type>(schema_type),
          ts_field_id);
        auto pk_copy = pk.copy();
        ASSERT_EQ(pk_copy, pk);
        for (const auto& f : pk_copy.val->fields) {
            ASSERT_TRUE(f.has_value());
        }
    }
}
