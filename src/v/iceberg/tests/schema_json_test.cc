// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/json_writer.h"
#include "iceberg/schema.h"
#include "iceberg/schema_json.h"
#include "iceberg/tests/test_schemas.h"
#include "json/document.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(SchemaJsonSerde, TestNestedSchema) {
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_nested_schema_json_str);
    auto parsed_schema = parse_schema(parsed_orig_json);

    field_type expected_type = test_nested_schema_type();
    ASSERT_EQ(
      parsed_schema.schema_struct, std::get<struct_type>(expected_type));
    ASSERT_EQ(parsed_schema.schema_id(), 1);
    ASSERT_EQ(parsed_schema.identifier_field_ids.size(), 1);
    ASSERT_EQ((*parsed_schema.identifier_field_ids.begin())(), 1);

    const ss::sstring parsed_orig_as_str = to_json_str(parsed_schema);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    auto parsed_roundtrip_schema = parse_schema(parsed_roundtrip_json);

    ASSERT_EQ(parsed_roundtrip_schema, parsed_schema);
}
