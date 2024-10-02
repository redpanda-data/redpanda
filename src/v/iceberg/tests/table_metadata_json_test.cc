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
#include "iceberg/tests/test_table_metadata.h"

#include <gtest/gtest.h>

using namespace iceberg;

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
