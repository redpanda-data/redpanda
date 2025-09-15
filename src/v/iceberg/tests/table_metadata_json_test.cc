/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility_utils.h"
#include "iceberg/json_writer.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_metadata_json.h"
#include "iceberg/tests/test_table_metadata.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {
// Sample metadata created by Unity Catalog (tweaked location).
auto uc_metadata_json = R"JSON({
  "format-version" : 2,
  "table-uuid" : "1a09d6a4-a3ca-45e9-a537-465e36564895",
  "location" : "s3://redpanda-bucket/location",
  "last-sequence-number" : 0,
  "last-updated-ms" : 1743125941877,
  "last-column-id" : 10,
  "current-schema-id" : 0,
  "schemas" : [ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "redpanda",
      "required" : true,
      "type" : {
        "type" : "struct",
        "fields" : [ {
          "id" : 3,
          "name" : "partition",
          "required" : true,
          "type" : "int"
        }, {
          "id" : 4,
          "name" : "offset",
          "required" : true,
          "type" : "long"
        }, {
          "id" : 5,
          "name" : "timestamp",
          "required" : true,
          "type" : "timestamp"
        }, {
          "id" : 6,
          "name" : "headers",
          "required" : false,
          "type" : {
            "type" : "list",
            "element-id" : 8,
            "element" : {
              "type" : "struct",
              "fields" : [ {
                "id" : 9,
                "name" : "key",
                "required" : false,
                "type" : "binary"
              }, {
                "id" : 10,
                "name" : "value",
                "required" : false,
                "type" : "binary"
              } ]
            },
            "element-required" : true
          }
        }, {
          "id" : 7,
          "name" : "key",
          "required" : false,
          "type" : "binary"
        } ]
      }
    }, {
      "id" : 2,
      "name" : "value",
      "required" : false,
      "type" : "binary"
    } ]
  } ],
  "default-spec-id" : 0,
  "partition-specs" : [ {
    "spec-id" : 0,
    "fields" : [ {
      "name" : "redpanda.timestamp_hour",
      "transform" : "hour",
      "source-id" : 5,
      "field-id" : 1000
    } ]
  } ],
  "last-partition-id" : 1000,
  "default-sort-order-id" : 0,
  "sort-orders" : [ {
    "order-id" : 0,
    "fields" : [ ]
  } ],
  "properties" : {
    "write.object-storage.enabled" : "true",
    "write.metadata.compression-codec" : "gzip",
    "write.summary.partition-limit" : "100",
    "write.parquet.compression-codec" : "zstd"
  },
  "current-snapshot-id" : null,
  "refs" : { },
  "snapshots" : [ ],
  "statistics" : [ ],
  "partition-statistics" : [ ],
  "snapshot-log" : [ ],
  "metadata-log" : [ ]
})JSON";
} // anonymous namespace

// Parse metadata from Unity Catalog.
TEST(TableMetadataJsonSerde, TestUCMetadata) {
    const auto test_str = uc_metadata_json;
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_table_meta(parsed_orig_json);
    ASSERT_EQ(format_version::v2, parsed.format_version);
    ASSERT_EQ(
      uuid_t::from_string("1a09d6a4-a3ca-45e9-a537-465e36564895"),
      parsed.table_uuid);
    ASSERT_EQ("s3://redpanda-bucket/location", parsed.location);
    ASSERT_EQ(0, parsed.last_sequence_number());
    ASSERT_EQ(1743125941877, parsed.last_updated_ms.value());
    ASSERT_EQ(10, parsed.last_column_id());
    ASSERT_EQ(0, parsed.current_schema_id());
    ASSERT_EQ(1, parsed.schemas.size());
    ASSERT_EQ(2, parsed.schemas[0].schema_struct.fields.size());
    ASSERT_EQ(0, parsed.default_spec_id());
    ASSERT_EQ(1, parsed.partition_specs.size());
    ASSERT_EQ(0, parsed.partition_specs[0].spec_id());
    ASSERT_EQ(1, parsed.partition_specs[0].fields.size());
    ASSERT_EQ(1000, parsed.last_partition_id());
    ASSERT_EQ(0, parsed.default_sort_order_id());
    ASSERT_EQ(1, parsed.sort_orders.size());
    ASSERT_EQ(0, parsed.sort_orders[0].order_id());
    ASSERT_EQ(0, parsed.sort_orders[0].fields.size());
    ASSERT_TRUE(parsed.properties.has_value());
    ASSERT_EQ(4, parsed.properties->size());

    // Regression test for not handling "null".
    ASSERT_FALSE(parsed.current_snapshot_id.has_value());

    ASSERT_TRUE(parsed.snapshots.has_value());
    ASSERT_EQ(0, parsed.snapshots->size());
    ASSERT_TRUE(parsed.refs.has_value());
    ASSERT_TRUE(parsed.refs->empty());
}

TEST(TableMetadataJsonSerde, TestTableMetadata) {
    const auto test_str = test_table_meta_json;
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_table_meta(parsed_orig_json);
    ASSERT_EQ(format_version::v2, parsed.format_version);
    ASSERT_EQ(
      uuid_t::from_string("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
      parsed.table_uuid);
    ASSERT_EQ("s3://foo/bar/baz", parsed.location);
    ASSERT_EQ(34, parsed.last_sequence_number());
    ASSERT_EQ(1602638573590, parsed.last_updated_ms.value());
    ASSERT_EQ(3, parsed.last_column_id());
    ASSERT_EQ(1, parsed.current_schema_id());
    ASSERT_EQ(2, parsed.schemas.size());
    ASSERT_EQ(1, parsed.schemas[0].schema_struct.fields.size());
    ASSERT_EQ(4, parsed.schemas[1].schema_struct.fields.size());
    ASSERT_EQ(0, parsed.default_spec_id());
    ASSERT_EQ(1, parsed.partition_specs.size());
    ASSERT_EQ(0, parsed.partition_specs[0].spec_id());
    ASSERT_EQ(1, parsed.partition_specs[0].fields.size());
    ASSERT_EQ(1000, parsed.last_partition_id());
    ASSERT_EQ(3, parsed.default_sort_order_id());
    ASSERT_EQ(1, parsed.sort_orders.size());
    ASSERT_EQ(3, parsed.sort_orders[0].order_id());
    ASSERT_EQ(2, parsed.sort_orders[0].fields.size());
    ASSERT_TRUE(parsed.properties.has_value());
    ASSERT_EQ(1, parsed.properties->size());
    ASSERT_TRUE(parsed.current_snapshot_id.has_value());
    ASSERT_EQ(3055729675574597004, parsed.current_snapshot_id.value()());
    ASSERT_TRUE(parsed.snapshots.has_value());
    ASSERT_EQ(2, parsed.snapshots->size());
    ASSERT_TRUE(parsed.refs.has_value());
    ASSERT_TRUE(parsed.refs->contains("main"));
    ASSERT_TRUE(parsed.refs->contains("foo"));

    const auto parsed_orig_as_str = iceberg::to_json_str(parsed);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip = parse_table_meta(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip, parsed);
}

TEST(TableMetadataJsonSerde, TestTableMetadataNoOptionals) {
    const auto test_str = test_table_meta_no_optionals_json;
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_table_meta(parsed_orig_json);
    ASSERT_EQ(format_version::v2, parsed.format_version);
    ASSERT_EQ(
      uuid_t::from_string("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
      parsed.table_uuid);
    ASSERT_EQ("s3://foo/bar/baz", parsed.location);
    ASSERT_EQ(34, parsed.last_sequence_number());
    ASSERT_EQ(1602638573590, parsed.last_updated_ms.value());
    ASSERT_EQ(3, parsed.last_column_id());
    ASSERT_EQ(1, parsed.current_schema_id());
    ASSERT_EQ(2, parsed.schemas.size());
    ASSERT_EQ(1, parsed.schemas[0].schema_struct.fields.size());
    ASSERT_EQ(4, parsed.schemas[1].schema_struct.fields.size());
    ASSERT_EQ(0, parsed.default_spec_id());
    ASSERT_EQ(1, parsed.partition_specs.size());
    ASSERT_EQ(0, parsed.partition_specs[0].spec_id());
    ASSERT_EQ(1, parsed.partition_specs[0].fields.size());
    ASSERT_EQ(1000, parsed.last_partition_id());
    ASSERT_EQ(3, parsed.default_sort_order_id());
    ASSERT_EQ(1, parsed.sort_orders.size());
    ASSERT_EQ(3, parsed.sort_orders[0].order_id());
    ASSERT_EQ(2, parsed.sort_orders[0].fields.size());

    const auto parsed_orig_as_str = iceberg::to_json_str(parsed);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip = parse_table_meta(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip, parsed);
}

TEST(TableMetadataJsonSerde, TestSchemaLookup) {
    const auto test_str = test_table_meta_json;
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_table_meta(parsed_orig_json);
    ASSERT_EQ(format_version::v2, parsed.format_version);

    auto* schema_by_id = parsed.get_schema(parsed.current_schema_id);
    ASSERT_NE(schema_by_id, nullptr);

    auto* schema_by_struct = parsed.get_equivalent_schema(
      schema_by_id->schema_struct);
    ASSERT_NE(schema_by_struct, nullptr);

    EXPECT_EQ(*schema_by_id, *schema_by_struct);
    EXPECT_TRUE(
      iceberg::schemas_equivalent(
        schema_by_id->schema_struct, schema_by_struct->schema_struct));
}
