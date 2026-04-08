/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/ir_json.h"
#include "iceberg/conversion/json_schema/frontend.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "iceberg/conversion/schema_json.h"
#include "iceberg/conversion/tests/gmock_iceberg_matchers.h"
#include "iceberg/conversion/values_json.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "json/document.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <rapidjson/error/en.h>

#include <limits>
#include <string_view>
#include <utility>
#include <variant>

using namespace ::testing;
using namespace iceberg;
using namespace iceberg::testing;

namespace {
AssertionResult field_matches(
  const iceberg::nested_field_ptr& field,
  const ss::sstring& name,
  const iceberg::field_type& ft,
  iceberg::field_required required) {
    if (
      field->id != 0 || field->name != name || field->type != ft
      || field->required != required) {
        return AssertionFailure() << fmt::format(
                 "\nexpected: (id: 0, name: {}, type: {}, required: {})\n"
                 "actual  : (id: {}, name: {}, type: {}, required: {})",
                 name,
                 ft,
                 required,
                 field->id,
                 field->name,
                 field->type,
                 field->required);
    }
    return AssertionSuccess();
}

conversion_outcome<json_conversion_ir>
to_iceberg_ir(std::string_view json_str) {
    json::Document doc;
    doc.Parse(json_str.data(), json_str.size());

    if (doc.HasParseError()) {
        return conversion_exception(
          fmt::format(
            "Failed to parse JSON schema: {}",
            rapidjson::GetParseError_En(doc.GetParseError())));
    }

    try {
        auto root = iceberg::conversion::json_schema::frontend().compile(
          doc, "https://example.com/arbitrary-base-uri.json", std::nullopt);
        return iceberg::type_to_ir(root);
    } catch (const std::exception& e) {
        return conversion_exception(
          fmt::format("Failed to convert JSON schema: {}", e.what()));
    }
}

conversion_outcome<iceberg::struct_type>
to_iceberg_type(std::string_view json_str) {
    auto iceberg_ir_res = to_iceberg_ir(json_str);
    if (iceberg_ir_res.has_error()) {
        return iceberg_ir_res.error();
    }

    return iceberg::type_to_iceberg(iceberg_ir_res.value());
}

ss::future<value_outcome>
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
to_iceberg_value(std::string_view json_schema_str, std::string_view json_str) {
    auto iceberg_ir_res = to_iceberg_ir(json_schema_str);
    if (iceberg_ir_res.has_error()) {
        co_return value_conversion_exception(
          fmt::format(
            "Failed to convert JSON schema to IR: {}",
            iceberg_ir_res.error().what()));
    }

    co_return co_await iceberg::deserialize_json(
      iobuf::from(json_str), iceberg_ir_res.value());
}

} // namespace

constexpr std::string_view schema_root_empty = R"({
  "$schema": "http://json-schema.org/draft-07/schema#"
})";
constexpr std::string_view schema_root_empty_types = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": []
})";

constexpr std::string_view schema_root_null = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "null"
})";

constexpr std::string_view schema_root_null_null = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["null", "null"]
})";

constexpr std::string_view schema_root_primitive = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "integer"
})";

constexpr std::string_view schema_root_primitive_array = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer"]
})";

constexpr std::string_view schema_root_primitive_array_with_null_first = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["null", "integer"]
})";

constexpr std::string_view schema_root_primitive_array_with_null_second = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer", "null"]
})";

constexpr std::string_view schema_root_primitive_array_or_string = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer", "string"]
})";

constexpr std::string_view nested_schema = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/product.schema.json",
  "title": "Product",
  "description": "A product from Acme's catalog",
  "type": "object",
  "properties": {
    "productId": {
      "description": "The unique identifier for a product",
      "type": "integer"
    },
    "productName": {
      "description": "Name of the product",
      "type": "string"
    },
    "price": {
      "description": "The price of the product",
      "type": "number",
      "exclusiveMinimum": 0
    },
    "tags": {
      "description": "Tags for the product",
      "type": "array",
      "items": {
        "type": "string"
      },
      "minItems": 1,
      "uniqueItems": true
    },
    "dimensions": {
      "type": "object",
      "properties": {
        "length": {
          "type": "number"
        },
        "width": {
          "type": "number"
        },
        "height": {
          "type": "number"
        }
      },
      "required": [ "length", "width", "height" ]
    }
  },
  "required": [ "productId", "productName", "price" ]
})";

// Obfuscated schema sample from a customer.
constexpr std::string_view nested_schema_2 = R"({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "sample schema",
    "type": "object",
    "description": "sample schema description",
    "properties": {
        "key1": {
            "description": "key1 description",
            "type": [
                "string"
            ]
        },
        "key2": {
            "description": "nullable array of nullable strings",
            "items": {
                "type": [
                    "string",
                    "null"
                ]
            },
            "type": [
                "array",
                "null"
            ]
        },
        "key3": {
            "description": "nullable string",
            "type": [
                "string",
                "null"
            ]
        },
        "key4": {
            "description": "an integer",
            "type": [
                "integer"
            ]
        },
        "key5": {
            "description": "a string",
            "type": [
                "string"
            ]
        },
        "key6": {
            "description": "nullable array of objects",
            "items": {
                "type": "object",
                "properties": {
                    "key1": {
                        "type": [
                            "string",
                            "null"
                        ]
                    },
                    "key2": {
                        "format": "date-time",
                        "type": [
                            "string",
                            "null"
                        ]
                    },
                    "key3": {
                        "type": "string",
                        "format": "email"
                    }
                }
            },
            "type": [
                "array",
                "null"
            ]
        }
    },
    "required": [
        "key1",
        "key2",
        "key3",
        "key4",
        "key5",
        "key6"
    ]
})";

TEST(JsonSchema, Empty) {
    for (const auto& schema_str : {
           schema_root_empty,
           schema_root_empty_types,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_error());
        ASSERT_STREQ(
          "Type constraint is not sufficient for transforming. Types: [null, "
          "boolean, object, array, number, integer, string]",
          result.error().what());
    }
}

TEST(JsonSchema, NullType) {
    for (const auto& schema_str : {
           schema_root_null,
           schema_root_null_null,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_error());
        ASSERT_STREQ(
          "Type constraint is not sufficient for transforming. Types: [null]",
          result.error().what());
    }
}

TEST(JsonSchema, PrimitiveTypes) {
    for (const auto& schema_str : {
           schema_root_primitive,
           schema_root_primitive_array,
           schema_root_primitive_array_with_null_first,
           schema_root_primitive_array_with_null_second,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_value()) << result.error().what();

        ASSERT_EQ(result.value().fields.size(), 1);
        ASSERT_TRUE(field_matches(
          result.value().fields[0],
          "root",
          iceberg::long_type{},
          iceberg::field_required::no));
    }
}

TEST(JsonSchema, PrimitiveTypesMixed) {
    auto result = to_iceberg_type(schema_root_primitive_array_or_string);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [integer, "
      "string]",
      result.error().what());
}

TEST(JsonSchema, ThreeWayAmbiguousUnion) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": ["boolean", "integer", "string"]
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [boolean, "
      "integer, string]",
      result.error().what());
}

TEST(JsonSchema, NullableAmbiguousUnion) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": ["null", "integer", "string"]
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "integer, string]",
      result.error().what());
}

TEST(JsonSchema, ContainerTypeUnion) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": ["object", "array"]
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [object, "
      "array]",
      result.error().what());
}

TEST(JsonSchema, Nested) {
    auto result = to_iceberg_type(nested_schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 5);
}

TEST(JsonSchema, Nested2) {
    auto result = to_iceberg_type(nested_schema_2);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 6);

    EXPECT_TRUE(field_matches(
      result.value().fields[0],
      "key1",
      iceberg::string_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[1],
      "key2",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[2],
      "key3",
      iceberg::string_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[3],
      "key4",
      iceberg::long_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[4],
      "key5",
      iceberg::string_type{},
      iceberg::field_required::no));

    {
        EXPECT_TRUE(
          result.value().fields[5]->required == iceberg::field_required::no);
        EXPECT_TRUE(
          std::holds_alternative<iceberg::list_type>(
            result.value().fields[5]->type));

        if (
          std::holds_alternative<iceberg::list_type>(
            result.value().fields[5]->type)) {
            auto& item_type = std::get<iceberg::list_type>(
                                result.value().fields[5]->type)
                                .element_field;

            auto key6_struct = iceberg::struct_type{};
            key6_struct.fields.push_back(
              iceberg::nested_field::create(
                0,
                "key1",
                iceberg::field_required::no,
                iceberg::string_type{}));
            key6_struct.fields.push_back(
              iceberg::nested_field::create(
                0,
                "key2",
                iceberg::field_required::no,
                iceberg::timestamptz_type{}));
            key6_struct.fields.push_back(
              iceberg::nested_field::create(
                0,
                "key3",
                iceberg::field_required::no,
                iceberg::string_type{}));

            EXPECT_TRUE(field_matches(
              item_type,
              "element",
              iceberg::field_type(std::move(key6_struct)),
              iceberg::field_required::yes));
        }
    }
}

TEST(JsonSchema, ObjectWithInvalidProperty) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "age": {}
      }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "boolean, object, array, number, integer, string]",
      result.error().what());
}

TEST(JsonSchema, ObjectWithDuplicateProperty) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "name": { "type": "integer" }
      }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Duplicate property key: name",
      result.error().what());
}

TEST(JsonSchema, ObjectWithBooleanProperty) {
    constexpr std::string_view schema = R"({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/root.json",
    "type": "object",
    "properties": {
      "is_active": false
    }
  })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "boolean, object, array, number, integer, string]",
      result.error().what());
}

TEST(JsonSchema, ListWithoutItem) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array"
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Cannot convert JSON schema list type without items",
      result.error().what());
}

TEST(JsonSchema, ListWithInvalidItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": {}
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "boolean, object, array, number, integer, string]",
      result.error().what());
}

TEST(JsonSchema, ListWithInvalidItemsList) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{}]
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "boolean, object, array, number, integer, string]",
      result.error().what());
}

TEST(JsonSchema, ListWithItem) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": { "type": "string" }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithNullableItem) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": { "type": ["null", "string"] }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithEmptyItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": []
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the type defined in JSON schema",
      result.error().what());
}

TEST(JsonSchema, ListWithItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" },
          { "type": "string" }
      ]
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemsMixed) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" },
          { "type": "integer" }
      ]
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the same type, but found string and long",
      result.error().what());
}

TEST(JsonSchema, ListWithItemAndAdditionalItems) {
    // Per spec, additionalItems keyword must be ignored if items is
    // specified as an object.
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": { "type": "string" },
      "additionalItems": { "type": "integer" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemAndAdditionalItemsMatching) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{ "type": "string" }],
      "additionalItems": { "type": "string" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemAndInvalidAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{ "type": "string" }],
      "additionalItems": {}
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [null, "
      "boolean, object, array, number, integer, string]",
      result.error().what());
}

TEST(JsonSchema, ListWithItemsAndConflictingAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" }
      ],
      "additionalItems": { "type": "boolean" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the same type, but found string and boolean",
      result.error().what());
}

TEST(JsonSchema, ListWithEmptyItemsAndAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [],
      "additionalItems": { "type": "integer" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::long_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, Format) {
    // table from format to iceberg type
    constexpr auto test_cases
      = std::to_array<std::pair<std::string_view, iceberg::primitive_type>>({
        {"date-time", iceberg::timestamptz_type{}},
        {"date", iceberg::date_type{}},
        {"time", iceberg::time_type{}},
      });

    for (const auto& [format, type] : test_cases) {
        SCOPED_TRACE(fmt::format("Testing format: {}", format));
        constexpr std::string_view schema_template = R"({{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://example.com/root.json",
          "type": "string",
          "format": "{}"
        }})";

        auto schema = fmt::format(fmt::runtime(schema_template), format);
        auto result = to_iceberg_type(schema);
        ASSERT_TRUE(result.has_value()) << result.error().what();
        ASSERT_EQ(result.value().fields.size(), 1);
        ASSERT_TRUE(field_matches(
          result.value().fields[0], "root", type, iceberg::field_required::no));
    }
}

TEST(JsonSchema, FormatIgnoredOnNonStringTypes) {
    // Format annotations should be silently ignored on non-string types
    // per JSON Schema spec (format is only meaningful for strings).
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "number",
      "format": "date"
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::double_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, RefInternalSubschema) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "definitions": {
        "positiveInteger": {
          "type": "integer",
          "minimum": 0,
          "exclusiveMinimum": true
        }
      },
      "type": "object",
      "properties": {
        "age": { "$ref": "#/definitions/positiveInteger" }
      }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "age",
      iceberg::long_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, RefInfiniteRecursion) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "definitions": {
        "node": {
          "type": "object",
          "properties": {
            "value": { "type": "integer" },
            "next": { "$ref": "#/definitions/node" }
          }
        }
      },
      "type": "object",
      "properties": {
        "head": { "$ref": "#/definitions/node" }
      }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Schema depth limit exceeded during "
      "constraint collection",
      result.error().what());
}

TEST(JsonSchema, RequiresDraft7Schema) {
    // Test without $schema keyword
    constexpr std::string_view schema_without_schema = R"({
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    auto result = to_iceberg_type(schema_without_schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Schema dialect is not set (missing "
      "$schema keyword?)",
      result.error().what());

    // Test with incorrect schema version
    constexpr std::string_view schema_with_wrong_version = R"({
      "$schema": "http://json-schema.org/draft-04/schema#",
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    result = to_iceberg_type(schema_with_wrong_version);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Unsupported JSON Schema feature: "
      "Unsupported JSON Schema dialect: "
      "http://json-schema.org/draft-04/schema#",
      result.error().what());

    // Test with correct schema version
    constexpr std::string_view schema_with_correct_version = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    result = to_iceberg_type(schema_with_correct_version);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    // Test with nested JSON Schemas
    constexpr std::string_view nested_schema_with_mixed_versions = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/nested.json",
      "type": "object",
      "properties": {
        "field1": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "string"
        },
        "field2": { "type": "integer" }
      }
    })";

    result = to_iceberg_type(nested_schema_with_mixed_versions);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Unsupported JSON Schema feature: "
      "Unsupported JSON Schema dialect: "
      "http://json-schema.org/draft-04/schema#",
      result.error().what());
}

TEST(JsonConversionIr, DeepCopy) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" },
        "field2": { "type": "integer" }
      }
    })";

    auto result = to_iceberg_ir(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    auto ir2 = result.value();

    auto& v = std::get<struct_type>(result.value().root()).fields[0];
    v->name = "field1_modified";

    ASSERT_EQ(std::get<struct_type>(ir2.root()).fields[0]->name, "field1");
}

TEST(JsonConversionIr, StructFieldIndex) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" },
        "field2": { "type": "integer" }
      }
    })";

    auto result = to_iceberg_ir(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().struct_field_map().at("field1").field_pos, 0);
    ASSERT_EQ(result.value().struct_field_map().at("field2").field_pos, 1);
}

TEST_CORO(IcebergValues, ValueNull) {
    // This test passes even without extending the type with a null, i.e.
    // `{"type": ["string", "null"]}` because we make all types nullable
    // by default.
    //
    // In theory, a null value shouldn't be allowed for this schema but we
    // allow it because we are explicitly not a JSON Schema validator.
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "string"
    })";

    auto result = co_await to_iceberg_value(schema, "null");

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    ASSERT_EQ_CORO(result_value->fields.size(), 1);
    ASSERT_FALSE_CORO(result_value->fields[0].has_value());
}

TEST_CORO(IcebergValues, ValuePrimitives) {
    const auto json_primitives = std::to_array<
      std::tuple<std::string_view, std::string_view, iceberg::value>>({
      {"boolean", "true", iceberg::boolean_value(true)},
      {"boolean", "false", iceberg::boolean_value(false)},
      {"integer", "42", iceberg::long_value(42)},
      {"integer", "42.0", iceberg::long_value(42)},
      {"integer", "4.2e1", iceberg::long_value(42)},
      {"integer", "-42", iceberg::long_value(-42)},
      {"integer",
       "9223372036854775807",
       iceberg::long_value(std::numeric_limits<int64_t>::max())},
      {"integer",
       "-9223372036854775808",
       iceberg::long_value(std::numeric_limits<int64_t>::min())},
      // Integer.{MIN,MAX}_SAFE_INTEGER in JavaScript. No guarantees beyond
      // this range for now because of parser limitations.
      {"integer",
       "9.007199254740991e15",
       iceberg::long_value(9007199254740991)},
      {"integer",
       "-9.007199254740991e15",
       iceberg::long_value(-9007199254740991)},
      {"number", "42", iceberg::double_value(42)},
      {"number", "3.14", iceberg::double_value(3.14)},
      {"string", R"("foo")", iceberg::string_value(iobuf::from("foo"))},
    });

    for (const auto& [type, value, expected] : json_primitives) {
        SCOPED_TRACE(fmt::format("Testing type: {}, value: {}", type, value));

        auto schema = fmt::format(
          R"({{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "{}"
          }})",
          type);
        auto result = co_await to_iceberg_value(
          schema, fmt::format("{}", value));

        ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
        auto result_value = std::get<std::unique_ptr<struct_value>>(
          std::move(result.value()));

        EXPECT_EQ(*result_value->fields[0], expected);
    }
}

TEST_CORO(IcebergValues, ValuePrimitivesInvalid) {
    const auto json_primitives = std::to_array<
      std::tuple<std::string_view, std::string_view, std::string>>({
      {"integer",
       "42.1",
       "Cannot convert non-integer double value 42.1 to integer without "
       "precision loss"},
      {"integer",
       "42e100",
       "Cannot convert non-integer double value 4.2e+101 to integer without "
       "precision loss"},
      // No support for 2^63 with scientific notation because json parser(s) see
      // a double.
      {"integer",
       "9.223372036854775807e18",
       "Cannot convert non-integer double value 9.223372036854778e+18 to "
       "integer without precision loss"},
    });

    for (const auto& [type, value, err] : json_primitives) {
        SCOPED_TRACE(fmt::format("Testing type: {}, value: {}", type, value));

        auto schema = fmt::format(
          R"({{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "{}"
          }})",
          type);
        auto result = co_await to_iceberg_value(
          schema, fmt::format("{}", value));

        ASSERT_TRUE_CORO(result.has_error());
        ASSERT_STREQ_CORO(err.c_str(), result.error().what());
    }
}

TEST_CORO(IcebergValues, ValueList) {
    // TODO: consider if we should allow other primitives like boolean.
    //   Our streaming parser currently disallows them because this
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": {"type": "string"}
    })";
    auto value = R"(["foo", "bar", "baz"])";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(IcebergList(ElementsAre(
        OptionalIcebergPrimitive<string_value>("foo"),
        OptionalIcebergPrimitive<string_value>("bar"),
        OptionalIcebergPrimitive<string_value>("baz")))));
}

TEST_CORO(IcebergValues, ValueObject) {
    auto schema = R"({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "key1": {"type": ["string", "null"]},
            "key2": {"type": "integer"}
        }
    })";
    auto value = R"({"key1": "value1", "key2": 42})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        OptionalIcebergPrimitive<string_value>("value1"),
        OptionalIcebergPrimitive<long_value>(42)));
}

TEST_CORO(IcebergValues, ValueObjectDuplicateKeysLastWins) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
          "key1": {"type": "string"}
      }
    })";
    auto value = R"({"key1": "value1", "key1": "value2"})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(OptionalIcebergPrimitive<string_value>("value2")));
}

TEST_CORO(IcebergValues, ValueObjectOptionals) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
          "key1": {"type": ["string", "null"]},
          "key2": {"type": ["integer", "null"]},
          "nullkey": {"type": ["object", "null"]}
      }
    })";
    auto value = R"({"key1": "value1", "nullkey": null, "key3": "extra-key"})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        OptionalIcebergPrimitive<string_value>("value1"),
        Eq(std::nullopt),
        Eq(std::nullopt)));
}

TEST_CORO(IcebergValues, ValueObjectSpuriousCompoundMember) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
          "key1": {"type": ["string", "null"]},
          "key2": {"type": "integer"}
      }
    })";
    auto value
      = R"({"key1": "value1", "key2": 42, "key3": {"nested" : ["is", ["more"], "fun"]}})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        OptionalIcebergPrimitive<string_value>("value1"),
        OptionalIcebergPrimitive<long_value>(42)));
}

TEST_CORO(IcebergValues, ValueObjectNesting) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
          "key1": {"type": ["string", "null"]},
          "key2": {
              "type": "object",
              "properties": {
                  "key10": {
                      "type": ["array", "null"],
                      "items": {
                          "type": ["array"],
                          "items": {
                              "type": ["object"],
                              "properties": {
                                  "key100": {"type": ["boolean", "null"]},
                                  "key200": {"type": ["boolean", "null"]},
                                  "key300": {"type": ["boolean", "null"]}
                              }
                          }
                      }
                  },
                  "key20": {"type": ["string", "null"]}
              }
          }
      }
    })";
    auto value = R"({
      "key1": "value1",
      "key2": {
          "key20": "value2",
          "key10": [
              [
                  {
                      "key300": false,
                      "key100": true
                  }
              ]
          ]
      }
    })";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        // key1
        OptionalIcebergPrimitive<string_value>("value1"),
        // key2
        IcebergStruct(
          // key10
          IcebergList(ElementsAre(
            // key10 nested list
            IcebergList(ElementsAre(
              // key10 nested object
              IcebergStruct(
                // key100
                OptionalIcebergPrimitive<boolean_value>(true),
                // key200
                Eq(std::nullopt),
                // key300
                OptionalIcebergPrimitive<boolean_value>(false))
              // end of key10 nested list
              ))
            // end of key10
            )),
          // key20
          OptionalIcebergPrimitive<string_value>("value2"))));
}

TEST(JsonSchema, AdditionalProperties) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "additionalProperties": { "type": "string" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::map_type::create(
        0,
        iceberg::string_type{},
        0,
        iceberg::field_required::no,
        iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, AdditionalPropertiesWithProperties) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" }
      },
      "additionalProperties": { "type": "integer" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Cannot convert object with both properties and schema-valued "
      "additionalProperties",
      result.error().what());
}

TEST(JsonSchema, AdditionalPropertiesFalse) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" }
      },
      "additionalProperties": false
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field1",
      iceberg::string_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, AdditionalPropertiesTrue) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "additionalProperties": true
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Only 'false' or object subschema is supported for "
      "additionalProperties keyword",
      result.error().what());
}

TEST_CORO(IcebergValues, ValueObjectAdditionalProperties) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "additionalProperties": { "type": "string" }
    })";
    auto value = R"({"key1": "value1", "key2": null})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(IcebergMap(UnorderedElementsAre(
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key1"),
          OptionalIcebergPrimitive<string_value>("value1")),
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key2"), Eq(std::nullopt))))));
}

TEST_CORO(IcebergValues, ValueObjectAdditionalPropertiesDuplicateKeys) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "additionalProperties": { "type": "string" }
    })";
    auto value = R"({"key1": "value1", "key1": "value2"})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(IcebergMap(ElementsAre(IcebergKeyValue(
        IcebergPrimitive<string_value>("key1"),
        OptionalIcebergPrimitive<string_value>("value2"))))));
}

TEST_CORO(IcebergValues, ValueObjectAdditionalPropertiesEmpty) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "additionalProperties": { "type": "string" }
    })";
    auto value = R"({})";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(result_value->fields, ElementsAre(IcebergMap(IsEmpty())));
}

TEST_CORO(IcebergValues, ValueObjectAdditionalPropertiesList) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "additionalProperties": {
        "type": "array",
        "items": { "type": "string" }
      }
    })";
    auto value = R"({
      "key1": ["a", "b"],
      "key2": ["c"]
    })";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(IcebergMap(UnorderedElementsAre(
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key1"),
          IcebergList(ElementsAre(
            OptionalIcebergPrimitive<string_value>("a"),
            OptionalIcebergPrimitive<string_value>("b")))),
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key2"),
          IcebergList(
            ElementsAre(OptionalIcebergPrimitive<string_value>("c"))))))));
}

TEST_CORO(IcebergValues, ValueObjectAdditionalPropertiesNesting) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "tags": {
          "type": "object",
          "additionalProperties": {
            "type": "object",
            "properties": {
              "nested": { "type": "integer" }
            }
          }
        }
      }
    })";
    auto value = R"({
      "tags": {
        "key1": { "nested": 1 },
        "key2": { "nested": 2 }
      }
    })";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(IcebergMap(UnorderedElementsAre(
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key1"),
          IcebergStruct(OptionalIcebergPrimitive<long_value>(1))),
        IcebergKeyValue(
          IcebergPrimitive<string_value>("key2"),
          IcebergStruct(OptionalIcebergPrimitive<long_value>(2)))))));
}

TEST(JsonSchema, OneOfNullable) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "nullable_field": {
          "oneOf": [
            { "type": "string" },
            { "type": "null" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "nullable_field",
      iceberg::string_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfTypeRefinement) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "type": ["number", "string", "null"],
          "oneOf": [
            { "type": "null" },
            { "type": "string", "format": "date" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::date_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfArbitraryTypes) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "string" },
            { "type": "integer" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "oneOf keyword is supported only for exclusive T|null structures",
      result.error().what());
}

TEST(JsonSchema, OneOfThreeBranches) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "string" },
            { "type": "integer" },
            { "type": "null" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "oneOf keyword is supported only for exclusive T|null structures",
      result.error().what());
}

TEST(JsonSchema, OneOfConflictingTypes) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "type": "number",
          "oneOf": [
            { "type": "null" },
            { "type": "string" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: []",
      result.error().what());
}

TEST(JsonSchema, OneOfRedundantNull) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            { "type": ["string", "null"] }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "oneOf keyword is supported only for exclusive T|null structures",
      result.error().what());
}

TEST(JsonSchema, OneOfNullableFormatInside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            { "type": "string", "format": "date" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::date_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfNullableFormatOutside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            { "type": "string" }
          ],
          "format": "date"
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::date_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfNullableFormatInsideAndOutside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            { "type": "string", "format": "date" }
          ],
          "format": "date"
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::date_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfFormatConflict) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            { "type": "string", "format": "date" }
          ],
          "format": "date-time"
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Conflicting format annotations across branches", result.error().what());
}

TEST(JsonSchema, OneOfProperties) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "properties": {
                "nested_field": { "type": "string" }
              }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    auto expected_field = iceberg::struct_type{};
    expected_field.fields.push_back(
      iceberg::nested_field::create(
        0,
        "nested_field",
        iceberg::field_required::no,
        iceberg::string_type{}));
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::field_type(std::move(expected_field)),
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfPropertiesBothSides) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "properties": {
            "nested_field": { "type": "string" }
          },
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "properties": {
                "other_field": { "type": "integer" }
              }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Intersecting constraints with properties on both sides is not supported",
      result.error().what());
}

TEST(JsonSchema, OneOfPropertiesOutside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "properties": {
            "nested_field": { "type": "string" }
          },
          "oneOf": [
            { "type": "null" },
            { "type": "object" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    auto expected_field = iceberg::struct_type{};
    expected_field.fields.push_back(
      iceberg::nested_field::create(
        0,
        "nested_field",
        iceberg::field_required::no,
        iceberg::string_type{}));
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::field_type(std::move(expected_field)),
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfAdditionalPropertiesOutside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "properties": {
                "nested_field": { "type": "string" }
              }
            }
          ],
          "additionalProperties": false
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "additionalProperties: false conflicts with properties defined in "
      "another branch",
      result.error().what());
}

TEST(JsonSchema, OneOfItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            {
              "type": "array",
              "items": { "type": "string" }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfItemsOutside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "items": { "type": "string" },
          "oneOf": [
            { "type": "null" },
            { "type": "array" }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_value()) << result.error().what();
    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, OneOfItemsBothSides) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "items": { "type": "integer" },
          "oneOf": [
            { "type": "null" },
            {
              "type": "array",
              "items": { "type": "string" }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Intersecting constraints with items on both sides is not supported",
      result.error().what());
}

TEST(JsonSchema, OneOfAdditionalPropertiesInside) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "additionalProperties": false
            }
          ],
          "properties": {
            "nested_field": { "type": "string" }
          }
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "additionalProperties: false conflicts with properties defined in "
      "another branch",
      result.error().what());
}

TEST(JsonSchema, OneOfAdditionalPropertiesSchemaBothSides) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "additionalProperties": { "type": "string" },
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "additionalProperties": { "type": "integer" }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Intersecting constraints with additionalProperties on both sides is "
      "not supported",
      result.error().what());
}

TEST(JsonSchema, OneOfAdditionalPropertiesSchemaConflictsWithProperties) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "additionalProperties": { "type": "string" },
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "properties": {
                "nested_field": { "type": "integer" }
              }
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "additionalProperties schema conflicts with properties defined in "
      "another branch",
      result.error().what());
}

TEST(
  JsonSchema,
  OneOfAdditionalPropertiesFalseConflictsWithAdditionalPropertiesSchema) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "field": {
          "additionalProperties": { "type": "string" },
          "oneOf": [
            { "type": "null" },
            {
              "type": "object",
              "additionalProperties": false
            }
          ]
        }
      }
    })";

    auto result = to_iceberg_type(schema);

    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "additionalProperties: false conflicts with additionalProperties "
      "schema defined in another branch",
      result.error().what());
}

TEST_CORO(IcebergValues, Format) {
    const auto test_cases = std::to_array<
      std::tuple<std::string_view, std::string_view, iceberg::value>>({
      {"date-time",
       R"("2025-01-01T01:02:03Z")",
       iceberg::timestamptz_value{1735693323000000}},
      {"date", R"("1990-12-31")", iceberg::date_value{7669}},
      {"time", R"("01:02:03Z")", iceberg::time_value{3723000000}},
    });

    for (const auto& [format, value, expected] : test_cases) {
        SCOPED_TRACE(
          fmt::format("Testing format: {}, value: {}", format, value));

        auto schema = fmt::format(
          R"({{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "array",
          "items": {{"type": "string", "format": "{}"}}
          }})",
          format);
        auto result = co_await to_iceberg_value(
          schema, fmt::format("[{}]", value));

        ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
        auto result_value = std::get<std::unique_ptr<struct_value>>(
          std::move(result.value()));

        const auto& list = std::get<std::unique_ptr<iceberg::list_value>>(
          *result_value->fields[0]);

        EXPECT_EQ(list->elements.at(0), expected) << fmt::format(
          "Expected: {}, got: {}", expected, *list->elements.at(0));
    }

    SCOPED_TRACE("Testing invalid format");
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": {"type": "string", "format": "date"}
    })";
    auto value = R"(["2025-01-01T01:02:03Z"])";

    auto result = co_await to_iceberg_value(schema, value);
    ASSERT_TRUE_CORO(result.has_error());
    ASSERT_STREQ_CORO(
      result.error().what(),
      "Failed to parse date value '2025-01-01T01:02:03Z'");
}

TEST_CORO(IcebergValues, FormatValueTooLong) {
    auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": {"type": "string", "format": "date-time"}
    })";
    auto value
      = R"(["2025-01-01T01:02:03.111111111111111111111111111111111111111111111111Z"])";

    auto result = co_await to_iceberg_value(schema, value);
    ASSERT_TRUE_CORO(result.has_error());
    ASSERT_STREQ_CORO(
      result.error().what(),
      "String value exceeds maximum length of 64 bytes: 69");
}

TEST_CORO(IcebergValues, Empty) {
    const auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": {"type": "string"}
    })";
    auto value = R"()";

    auto result = co_await to_iceberg_value(schema, value);

    ASSERT_TRUE_CORO(result.has_error());
}

TEST_CORO(IcebergValues, MismatchedTypes) {
    const auto schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "array",
      "items": {"type": "string"}
    })";
    auto value = R"([42])";

    auto result = co_await to_iceberg_value(schema, value);
    ASSERT_TRUE_CORO(result.has_error());
    ASSERT_STREQ_CORO(
      result.error().what(),
      "Mismatch json between json integer value and schema type: string");
}

TEST_CORO(IcebergValues, TruncatedInputs) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "comment": { "type": "string" },
        " s p a c e d ": { "type": "array", "items": { "type": "integer" } }
      }
    })";

    // Inspired by json_checker_pass1.json.
    constexpr std::string_view input = R"({
      "integer": 1234567890,
      "real": -9876.543210,
      "e": 0.123456789e-12,
      "E": 1.234567890E+34,
      "":  23456789012E66,
      "zero": 0,
      "one": 1,
      "space": " ",
      "quote": "\"",
      "backslash": "\\",
      "controls": "\b\f\n\r\t",
      "slash": "/ & \/",
      "alpha": "abcdefghijklmnopqrstuvwyz",
      "ALPHA": "ABCDEFGHIJKLMNOPQRSTUVWYZ",
      "digit": "0123456789",
      "0123456789": "digit",
      "special": "`1~!@#$%^&*()_+-={':[,]}|;.</>?",
      "hex": "\u0123\u4567\u89AB\uCDEF\uabcd\uef4A",
      "true": true,
      "false": false,
      "null": null,
      "array":[  ],
      "object":{  },
      "address": "50 St. James Street",
      "url": "http://www.JSON.org/",
      "comment": "// /* <!-- --",
      "# -- --> */": " ",
      " s p a c e d " :[1,2 , 3

,

4 , 5        ,          6           ,7        ],"compact":[1,2,3,4,5,6,7],
      "jsontext": "{\"object with 1 member\":[\"array with 1 element\"]}",
      "quotes": "&#34; \u0022 %22 0x22 034 &#x22;",
      "\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?"
: "A key can be any string"
    })";

    for (size_t i = 0; i < input.size(); ++i) {
        SCOPED_TRACE(
          fmt::format(
            "Testing truncated input at position {} of {}", i, input.size()));

        // Truncate the input string
        auto truncated_input = input.substr(0, i);

        auto result = co_await to_iceberg_value(schema, truncated_input);
        ASSERT_TRUE_CORO(result.has_error());

        static_assert(
          std::is_same_v<decltype(result.error()), value_conversion_exception&>,
          "Expected value_conversion_exception");
    }

    SCOPED_TRACE("Testing full input");
    auto result = co_await to_iceberg_value(schema, input);
    ASSERT_TRUE_CORO(result.has_value()) << result.error().what();
}
