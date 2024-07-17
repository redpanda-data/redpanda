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
#include "json/document.h"
#include "json/stringbuffer.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {
ss::sstring type_to_json_str(const field_type& t) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> w(buf);
    rjson_serialize(w, t);
    return buf.GetString();
}
} // namespace

// Round trip test for the struct type of a schema taken from
// https://github.com/apache/iceberg-go/blob/704a6e78c13ea63f1ff4bb387f7d4b365b5f0f82/schema_test.go#L644
TEST(DataTypeJsonSerde, TestFieldType) {
    struct_type expected_struct;
    expected_struct.fields.emplace_back(
      nested_field::create(1, "foo", field_required::no, string_type{}));
    expected_struct.fields.emplace_back(
      nested_field::create(2, "bar", field_required::yes, int_type{}));
    expected_struct.fields.emplace_back(
      nested_field::create(3, "baz", field_required::no, boolean_type{}));

    expected_struct.fields.emplace_back(nested_field::create(
      4,
      "qux",
      field_required::yes,
      list_type::create(5, field_required::yes, string_type{})));

    expected_struct.fields.emplace_back(nested_field::create(
      6,
      "quux",
      field_required::yes,
      map_type::create(
        7,
        string_type{},
        8,
        field_required::yes,
        map_type::create(
          9, string_type{}, 10, field_required::yes, int_type{}))));

    struct_type location_struct;
    location_struct.fields.emplace_back(
      nested_field::create(13, "latitude", field_required::no, float_type{}));
    location_struct.fields.emplace_back(
      nested_field::create(14, "longitude", field_required::no, float_type{}));
    expected_struct.fields.emplace_back(nested_field::create(
      11,
      "location",
      field_required::yes,
      list_type::create(12, field_required::yes, std::move(location_struct))));

    struct_type person_struct;
    person_struct.fields.emplace_back(
      nested_field::create(16, "name", field_required::no, string_type{}));
    person_struct.fields.emplace_back(
      nested_field::create(17, "age", field_required::yes, int_type{}));
    expected_struct.fields.emplace_back(nested_field::create(
      15, "person", field_required::no, std::move(person_struct)));
    field_type expected_type = std::move(expected_struct);

    const char* json_data = R"JSON({
    "type": "struct",
    "schema-id": 1,
    "identifier-field-ids": [1],
    "fields": [
      {
        "type": "string",
        "id": 1,
        "name": "foo",
        "required": false
      },
      {
        "type": "int",
        "id": 2,
        "name": "bar",
        "required": true
      },
      {
        "type": "boolean",
        "id": 3,
        "name": "baz",
        "required": false
      },
      {
        "id": 4,
        "name": "qux",
        "required": true,
        "type": {
          "type": "list",
          "element-id": 5,
          "element-required": true,
          "element": "string"
        }
      },
      {
        "id": 6,
        "name": "quux",
        "required": true,
        "type": {
          "type": "map",
          "key-id": 7,
          "key": "string",
          "value-id": 8,
          "value": {
            "type": "map",
            "key-id": 9,
            "key": "string",
            "value-id": 10,
            "value": "int",
            "value-required": true
          },
          "value-required": true
        }
      },
      {
        "id": 11,
        "name": "location",
        "required": true,
        "type": {
          "type": "list",
          "element-id": 12,
          "element-required": true,
          "element": {
            "type": "struct",
            "fields": [
              {
                "id": 13,
                "name": "latitude",
                "type": "float",
                "required": false
              },
              {
                "id": 14,
                "name": "longitude",
                "type": "float",
                "required": false
              }
            ]
          }
        }
      },
      {
        "id": 15,
        "name": "person",
        "required": false,
        "type": {
          "type": "struct",
          "fields": [
            {
              "id": 16,
              "name": "name",
              "type": "string",
              "required": false
            },
            {
              "id": 17,
              "name": "age",
              "type": "int",
              "required": true
            }
          ]
        }
      }
    ]
  })JSON";
    const ss::sstring expected_type_str = type_to_json_str(expected_type);

    json::Document parsed_orig_json;
    parsed_orig_json.Parse(json_data);
    auto parsed_orig_type = parse_type(parsed_orig_json);
    const ss::sstring parsed_orig_as_str = type_to_json_str(parsed_orig_type);
    ASSERT_EQ(expected_type, parsed_orig_type)
      << fmt::format("{}\nvs\n{}", expected_type_str, parsed_orig_as_str);

    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    auto parsed_roundtrip_type_moved = parse_type(parsed_roundtrip_json);
    auto parsed_roundtrip_type = std::move(parsed_roundtrip_type_moved);
    const ss::sstring parsed_roundtrip_as_str = type_to_json_str(
      parsed_roundtrip_type);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_NE(parsed_roundtrip_type_moved, parsed_roundtrip_type);
    ASSERT_EQ(parsed_roundtrip_type, expected_type)
      << fmt::format("{}\nvs\n{}", parsed_roundtrip_as_str, expected_type_str);
}
