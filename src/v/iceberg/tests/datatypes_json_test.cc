// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes.h"
#include "iceberg/datatypes_json.h"
#include "iceberg/tests/test_schemas.h"
#include "json/document.h"

#include <gtest/gtest.h>

using namespace iceberg;

// Round trip test for the struct type of a schema taken from
// https://github.com/apache/iceberg-go/blob/704a6e78c13ea63f1ff4bb387f7d4b365b5f0f82/schema_test.go#L644
TEST(DataTypeJsonSerde, TestFieldType) {
    field_type expected_type = test_nested_schema_type();
    const ss::sstring expected_type_str = iceberg::to_json_str(expected_type);

    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_nested_schema_json_str);
    auto parsed_orig_type = parse_type(parsed_orig_json);
    const ss::sstring parsed_orig_as_str = iceberg::to_json_str(
      parsed_orig_type);
    ASSERT_EQ(expected_type, parsed_orig_type)
      << fmt::format("{}\nvs\n{}", expected_type_str, parsed_orig_as_str);

    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    auto parsed_roundtrip_type_moved = parse_type(parsed_roundtrip_json);
    auto parsed_roundtrip_type = std::move(parsed_roundtrip_type_moved);
    const ss::sstring parsed_roundtrip_as_str = iceberg::to_json_str(
      parsed_roundtrip_type);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_NE(parsed_roundtrip_type_moved, parsed_roundtrip_type);
    ASSERT_EQ(parsed_roundtrip_type, expected_type)
      << fmt::format("{}\nvs\n{}", parsed_roundtrip_as_str, expected_type_str);
}
