// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/json_writer.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_metadata_json.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(TableMetadataJsonSerde, TestTableMetadata) {
    // This metadata was mostly taken from the Iceberg Rust project.
    // https://github.com/apache/iceberg-rust/blob/7aa8bddeb09420b2f81a50112603de28aeaf3be7/crates/iceberg/testdata/example_table_metadata_v2.json
    const auto test_str = R"(
{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://foo/bar/baz",
  "last-sequence-number": 34,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 1,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1, 2],
      "fields": [
        {"id": 1, "name": "x", "required": true, "type": "long"},
        {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
        {"id": 3, "name": "z", "required": true, "type": "long"},
        {"id": 4, "name": "a", "required": true, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 3,
  "sort-orders": [
    {
      "order-id": 3,
      "fields": [
        {"transform": "identity", "source-ids": [2], "direction": "asc", "null-order": "nulls-first"},
        {"transform": "bucket[4]", "source-ids": [3], "direction": "desc", "null-order": "nulls-last"}
      ]
    }
  ],
  "properties": {"read.split.target.size": "134217728"},
  "current-snapshot-id": 3055729675574597004,
  "snapshots": [
    {
      "snapshot-id": 3051729675574597004,
      "timestamp-ms": 1515100955770,
      "sequence-number": 0,
      "summary": {"operation": "append"},
      "manifest-list": "s3://foo/bar/baz/1"
    },
    {
      "snapshot-id": 3055729675574597004,
      "parent-snapshot-id": 3051729675574597004,
      "timestamp-ms": 1555100955770,
      "sequence-number": 1,
      "summary": {"operation": "append"},
      "manifest-list": "s3://foo/bar/baz/2",
      "schema-id": 1
    }
  ],
  "refs": {
    "main": {
      "snapshot-id": 5937117119577207000,
      "type": "branch",
      "max-ref-age-ms": 255486129308,
      "max-snapshot-age-ms": 255486129307,
      "min-snapshots-to-keep": 12345
    },
    "foo": {
      "snapshot-id": 5937117119577207000,
      "type": "tag"
    }
  }
}
)";
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
    // Same schema as above but with optional fields removed.
    const auto test_str = R"(
{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://foo/bar/baz",
  "last-sequence-number": 34,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 1,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1, 2],
      "fields": [
        {"id": 1, "name": "x", "required": true, "type": "long"},
        {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
        {"id": 3, "name": "z", "required": true, "type": "long"},
        {"id": 4, "name": "a", "required": true, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 3,
  "sort-orders": [
    {
      "order-id": 3,
      "fields": [
        {"transform": "identity", "source-ids": [2], "direction": "asc", "null-order": "nulls-first"},
        {"transform": "bucket[4]", "source-ids": [3], "direction": "desc", "null-order": "nulls-last"}
      ]
    }
  ]
}
)";
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
