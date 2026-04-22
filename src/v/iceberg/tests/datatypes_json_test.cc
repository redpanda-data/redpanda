/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/datatypes.h"
#include "iceberg/datatypes_json.h"
#include "iceberg/json_writer.h"
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
    // Serialization must be stable: a second serialize→parse→serialize cycle
    // must produce identical JSON to the first. This catches asymmetries where
    // fields are written but silently dropped on read (e.g. initial-default,
    // write-default).
    ASSERT_EQ(parsed_orig_as_str, parsed_roundtrip_as_str);
}

// Optional fields must serialize "initial-default": null and "write-default":
// null. initial-default tells query engines what to substitute for missing
// columns in old data files; write-default tells writers what to use when no
// value is provided. Required fields must not include either.
TEST(DataTypeJsonSerde, TestDefaultsSerialized) {
    auto optional_field = nested_field::create(
      1, "opt", field_required::no, string_type{});
    auto required_field = nested_field::create(
      2, "req", field_required::yes, int_type{});

    const auto opt_json = to_json_str(*optional_field);
    const auto req_json = to_json_str(*required_field);

    json::Document opt_doc;
    opt_doc.Parse(opt_json.c_str());
    ASSERT_FALSE(opt_doc.HasParseError()) << opt_json;
    ASSERT_TRUE(opt_doc.HasMember("initial-default"))
      << "optional field must have initial-default; got: " << opt_json;
    ASSERT_TRUE(opt_doc["initial-default"].IsNull())
      << "optional field initial-default must be null; got: " << opt_json;
    ASSERT_TRUE(opt_doc.HasMember("write-default"))
      << "optional field must have write-default; got: " << opt_json;
    ASSERT_TRUE(opt_doc["write-default"].IsNull())
      << "optional field write-default must be null; got: " << opt_json;

    json::Document req_doc;
    req_doc.Parse(req_json.c_str());
    ASSERT_FALSE(req_doc.HasParseError()) << req_json;
    ASSERT_FALSE(req_doc.HasMember("initial-default"))
      << "required field must not have initial-default; got: " << req_json;
    ASSERT_FALSE(req_doc.HasMember("write-default"))
      << "required field must not have write-default; got: " << req_json;
}
