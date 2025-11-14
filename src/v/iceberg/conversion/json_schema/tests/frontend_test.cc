/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "gmock/gmock.h"
#include "iceberg/conversion/json_schema/frontend.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "json/document.h"

#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <optional>
#include <ranges>
#include <string_view>
#include <variant>

using namespace testing;
using namespace iceberg::conversion::json_schema;

json::Document parse_json(std::string_view json_str) {
    json::Document doc;
    doc.Parse(json_str.data(), json_str.size());
    if (doc.HasParseError()) {
        throw std::runtime_error(
          fmt::format(
            "JSON parse error: {} at offset {}",
            doc.GetParseError(),
            doc.GetErrorOffset()));
    }
    return doc;
}

/// See
/// https://json-schema.org/draft/2020-12/json-schema-core#name-schema-identification-examp
constexpr std::string_view schema_identification_example = R"({
    "$id": "https://example.com/root.json",
    "type": "string",
    "$defs": {
        "A": { "$anchor": "foo" },
        "B": {
            "$id": "other.json",
            "$defs": {
                "X": { "$anchor": "bar" },
                "Y": {
                    "$id": "t/inner.json",
                    "$anchor": "bar"
                }
            }
        },
        "C": {
            "$id": "urn:uuid:ee564b8a-7a87-4125-8c96-e9f123d6766f"
        }
    }
})";

class ir_tree_printer {
public:
    static std::string to_string(const schema& s) {
        return to_string(s.root());
    };

    static std::string to_string(const subschema& s, std::string path = "") {
        std::string result;

        if (path.empty()) {
            result += "# (document root)\n";
            path = "#";
        } else {
            result += path + "\n";
        }

        result += "  base uri: " + s.base().id() + "\n";
        result += fmt::format("  dialect: {}\n", s.base().dialect());

        if (s.boolean_subschema().has_value()) {
            result += fmt::format(
              "  bool: {}\n", s.boolean_subschema().value() ? "true" : "false");
        }

        if (!s.types().empty()) {
            result += "  types: "
                      + fmt::format("[{}]\n", fmt::join(s.types(), ", "));
        }

        auto sorted_keys = std::views::keys(s.subschemas())
                           | std::ranges::to<std::vector>();
        std::ranges::sort(sorted_keys);

        for (const auto& k : sorted_keys) {
            result += to_string(
              s.subschemas().at(k), fmt::format("{}/{}", path, k));
        }

        return result;
    };
};

TEST(frontend_test, compile_valid_schema) {
    frontend f;
    auto schema = f.compile(
      parse_json(schema_identification_example),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
#/$defs/A
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
#/$defs/B
  base uri: https://example.com/other.json
  dialect: http://json-schema.org/draft-07/schema#
#/$defs/B/$defs/X
  base uri: https://example.com/other.json
  dialect: http://json-schema.org/draft-07/schema#
#/$defs/B/$defs/Y
  base uri: https://example.com/t/inner.json
  dialect: http://json-schema.org/draft-07/schema#
#/$defs/C
  base uri: urn:uuid:ee564b8a-7a87-4125-8c96-e9f123d6766f
  dialect: http://json-schema.org/draft-07/schema#
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
}

TEST(frontend_test, recognized_format) {
    auto schema = frontend{}.compile(
      parse_json(R"(
{
  "$id": "https://example.com/root.json",
  "type": "string",
  "format": "date-time"
})"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    ASSERT_EQ(schema.root().format(), format::date_time);
}

TEST(frontend_test, unsupported_format) {
    auto schema = frontend{}.compile(
      parse_json(R"(
{
  "$id": "https://example.com/root.json",
  "type": "string",
  "format": "unsupported-format-name-123"
})"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    ASSERT_EQ(schema.root().format(), std::nullopt);
}

TEST(frontend_test, boolean_schema_true) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "array",
        "items": true
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [array]
#/items
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  bool: true
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
}

TEST(frontend_test, object_properties) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" }
        }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [object]
#/properties/age
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [integer]
#/properties/name
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
    ASSERT_EQ(schema.root().properties().size(), 2);
}

TEST(frontend_test, duplicate_properties_def) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
              "$id": "https://example.com/root.json",
              "type": "object",
              "properties": {
                  "name": { "type": "string" }
              },
              "properties": {
                  "name": { "type": "integer" }
              }
            })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Duplicate keyword: properties")));
}

TEST(frontend_test, object_additional_properties) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "object",
        "additionalProperties": { "type": "string" }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [object]
#/additionalProperties
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
    ASSERT_TRUE(schema.root().additional_properties().has_value());
    ASSERT_EQ(
      schema.root().additional_properties()->get().types(),
      std::vector{json_value_type::string});
}

TEST(frontend_test, array_items) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "array",
        "items": { "type": "string" }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [array]
#/items
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
    ASSERT_TRUE(
      std::holds_alternative<std::reference_wrapper<const subschema>>(
        schema.root().items()));
    ASSERT_EQ(
      std::get<std::reference_wrapper<const subschema>>(schema.root().items())
        .get()
        .types(),
      std::vector{json_value_type::string});
}

TEST(frontend_test, array_items_list) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "array",
        "items": [{ "type": "string" }, { "type": "integer" }]
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [array]
#/items/0
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
#/items/1
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [integer]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
    ASSERT_TRUE(
      std::holds_alternative<iceberg::conversion::json_schema::const_list_view>(
        schema.root().items()));

    auto l = std::get<iceberg::conversion::json_schema::const_list_view>(
      schema.root().items());
    ASSERT_EQ(l.at(0).types(), std::vector{json_value_type::string});
    ASSERT_EQ(l.at(1).types(), std::vector{json_value_type::integer});
}

TEST(frontend_test, array_items_and_additional_items) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "array",
        "items": { "type": "string" },
        "additionalItems": { "type": "integer" }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [array]
#/additionalItems
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [integer]
#/items
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
    ASSERT_TRUE(
      std::holds_alternative<std::reference_wrapper<const subschema>>(
        schema.root().items()));
    ASSERT_TRUE(schema.root().additional_items().has_value());
    ASSERT_EQ(
      schema.root().additional_items()->get().types(),
      std::vector{json_value_type::integer});
}

TEST(frontend_test, nested_arrays_and_objects) {
    frontend f;
    auto schema = f.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "object",
        "properties": {
            "users": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "age": { "type": "integer" }
                    }
                }
            }
        }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [object]
#/properties/users
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [array]
#/properties/users/items
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [object]
#/properties/users/items/properties/age
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [integer]
#/properties/users/items/properties/name
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
}

TEST(frontend_test, ref) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "$ref": "#/$defs/inner",
          "$defs": {
              "inner": {
                  "type": "string"
              }
          }
        })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("The $ref keyword is not allowed")));
}

TEST(frontend_test, multiple_schema_resources) {
    // Test that we correctly restore context.
    auto schema = frontend{}.compile(
      parse_json(R"({
        "$id": "https://example.com/root.json",
        "type": "object",
        "properties": {
            "inner1": {
                "$id": "https://foo.example.com/inner1.json",
                "type": "string"
            },
            "inner2": {
                "$id": "/inner2.json",
                "type": "integer"
            }
        }
    })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto expected = R"(# (document root)
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [object]
#/properties/inner1
  base uri: https://foo.example.com/inner1.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [string]
#/properties/inner2
  base uri: https://example.com/inner2.json
  dialect: http://json-schema.org/draft-07/schema#
  types: [integer]
)";

    ASSERT_EQ(expected, ir_tree_printer::to_string(schema));
}

TEST(frontend_test, non_object_or_boolean_subschema) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
              "$id": "https://example.com/root.json",
              "items": [[]]
            })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Subschema must be an object or a boolean")));
}

TEST(frontend_test, non_string_dialect) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({ "$schema": 123 })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Invalid type for keyword $schema. Expected one of: [string].")));
}

TEST(frontend_test, non_string_id) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({ "$id": 123 })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Invalid type for keyword $id. Expected one of: [string].")));
}

TEST(frontend_test, id_with_fragment) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json#fragment",
          "type": "string"
        })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("The base URI must not contain a fragment")));
}

TEST(frontend_test, duplicate_ids) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "$defs": {
              "inner": {
                  "$id": "/",
                  "type": "string"
              },
              "inner_duplicate": {
                  "$id": "/",
                  "type": "integer"
              }
          }
        })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Duplicate schema ID: https://example.com/")));
}

TEST(frontend_test, supported_dialects) {
    constexpr auto supported_dialects = std::to_array({
      dialect::draft7,
    });

    frontend f;

    for (const auto& [id, d] : dialect_by_schema_id) {
        std::optional<schema> schema;

        auto expect_exception = std::ranges::find(supported_dialects, d)
                                == supported_dialects.end();

        try {
            schema = f.compile(
              parse_json(fmt::format(R"({{"$schema": "{}"}})", id)),
              "https://example.com/irrelevant-base.json",
              std::nullopt);
        } catch (const std::runtime_error& e) {
            if (expect_exception) {
                ASSERT_THAT(
                  e.what(),
                  StrEq(
                    fmt::format(
                      "Unsupported JSON Schema feature: Unsupported JSON "
                      "Schema "
                      "dialect: {}",
                      d)));
                continue;
            }
            throw;
        }

        auto expected = fmt::format(
          R"(# (document root)
  base uri: https://example.com/irrelevant-base.json
  dialect: {}
)",
          d);

        ASSERT_EQ(expected, ir_tree_printer::to_string(schema.value()));
        ASSERT_EQ(schema->root().dialect(), d);
    }
}

TEST(frontend_test, no_dialect) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({})"),
            "https://example.com/irrelevant-base.json",
            std::nullopt);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Schema dialect is not set (missing $schema keyword?)")));
}

TEST(frontend_test, explicit_unknown_dialect) {
    EXPECT_THAT(
      [] {
          frontend{}.compile(
            parse_json(R"({
    "$schema": "https://example.com/draft-07/schema"
})"),
            "https://example.com/irrelevant-base.json",
            std::nullopt);
      },
      ThrowsMessage<std::runtime_error>(StrEq(
        "Fell off the end of a string-switch while matching: "
        "https://example.com/draft-07/schema")));
}

TEST(frontend_test, depth_limit) {
    constexpr std::string_view subschema_template = R"({{
      "type": "object",
      "properties": {{
          "nested": {}
      }}
    }})";

    std::function<std::string(size_t)> generate_nested_schema =
      [&](size_t depth) {
          if (depth == 0) {
              return std::string(R"({"type": "string"})");
          }
          return fmt::format(
            fmt::runtime(subschema_template),
            generate_nested_schema(depth - 1));
      };

    EXPECT_THAT(
      [&] {
          frontend{}.compile(
            parse_json(
              fmt::format(
                R"({{"$id": "https://example.com/root.json", "type": "object", "properties": {{"nested": {}}}}})",
                generate_nested_schema(32))),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(StrEq("Schema depth limit exceeded")));
}

TEST(frontend_test, non_object_root) {
    EXPECT_THAT(
      [] {
          frontend{}.compile(
            parse_json(R"(true)"),
            "https://example.com/irrelevant-base.json",
            std::nullopt);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("JSON Schema document must be an object")));
}
