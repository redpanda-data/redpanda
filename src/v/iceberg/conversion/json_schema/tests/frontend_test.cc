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

#include <fmt/format.h>
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
    "definitions": {
        "A": { "$anchor": "foo" },
        "B": {
            "$id": "other.json",
            "definitions": {
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
#/definitions/A
  base uri: https://example.com/root.json
  dialect: http://json-schema.org/draft-07/schema#
#/definitions/B
  base uri: https://example.com/other.json
  dialect: http://json-schema.org/draft-07/schema#
#/definitions/B/definitions/X
  base uri: https://example.com/other.json
  dialect: http://json-schema.org/draft-07/schema#
#/definitions/B/definitions/Y
  base uri: https://example.com/t/inner.json
  dialect: http://json-schema.org/draft-07/schema#
#/definitions/C
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

TEST(frontend_test, ref_local_with_fragment) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/definitions/inner"
          },
          "definitions": {
            "inner": {
              "type": "string"
            }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "#/definitions/inner");
    ASSERT_EQ(
      items_schema.get().ref(),
      &schema.root().subschemas().at("definitions/inner"));
}

TEST(frontend_test, ref_local_without_fragment) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "inner.json"
          },
          "definitions": {
            "inner": {
              "$id": "inner.json",
              "type": "string"
            }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "inner.json");
    ASSERT_EQ(
      items_schema.get().ref()->base().id(), "https://example.com/inner.json");
}

TEST(frontend_test, ref_external_not_found) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
              "$id": "https://example.com/schemas/root.json",
              "items": { "$ref": "types.json#/$defs/MyType" }
            })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(StrEq(
        "Unresolvable $ref: types.json#/$defs/MyType. Schema resource "
        "https://example.com/schemas/types.json not available")));
}

TEST(frontend_test, ref_relative_uri) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/schemas/root.json",
          "properties": {
            "fieldWithRsc": {
              "$id": "types.json",
              "type": "object",
              "definitions": {
                "MyType": { "type": "string" }
              }
            },
            "myField": {
              "$ref": "types.json#/definitions/MyType"
            }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    const auto& my_field = schema.root().properties().at("myField");
    ASSERT_EQ(my_field.ref_value(), "types.json#/definitions/MyType");
    ASSERT_EQ(my_field.ref()->types(), std::vector{json_value_type::string});
}

TEST(frontend_test, ref_absolute_uri) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "type": "object",
          "properties": {
            "externalSchema": {
              "$id": "https://other.example.com/schemas/types.json",
              "definitions": {
                  "ExternalType": { "type": "string" }
              }
            },
            "myField": {
              "$ref": "https://other.example.com/schemas/types.json#/definitions/ExternalType"
            }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    const auto& my_field = schema.root().properties().at("myField");
    ASSERT_EQ(
      my_field.ref_value(),
      "https://other.example.com/schemas/types.json#/definitions/ExternalType");
    ASSERT_EQ(my_field.ref()->types(), std::vector{json_value_type::string});
}

TEST(frontend_test, ref_with_sibling_keywords) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/definitions/Base",
            "type": "object",
            "properties": {
                "extraField": { "type": "integer" }
            }
          },
          "definitions": {
              "Base": { "type": "string" }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "#/definitions/Base");
    ASSERT_TRUE(items_schema.get().types().empty());
    ASSERT_TRUE(items_schema.get().properties().empty());
}

TEST(frontend_test, ref_with_encoded_pointer) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/definitions/foo~1bar"
          },
          "definitions": {
              "foo/bar": { "type": "string" }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "#/definitions/foo~1bar");
    ASSERT_EQ(
      items_schema.get().ref(),
      &schema.root().subschemas().at("definitions/foo/bar"));
}

TEST(frontend_test, ref_with_percent_encoded_chars) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/definitions/my%24type"
          },
          "definitions": {
              "my$type": { "type": "string" }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "#/definitions/my%24type");
    ASSERT_EQ(
      items_schema.get().ref(),
      &schema.root().subschemas().at("definitions/my$type"));
}

TEST(frontend_test, ref_with_unencoded_special_chars) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/definitions/my$type"
          },
          "definitions": {
              "my$type": { "type": "string" }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(items_schema.get().ref_value(), "#/definitions/my$type");
    ASSERT_EQ(
      items_schema.get().ref(),
      &schema.root().subschemas().at("definitions/my$type"));
}

TEST(frontend_test, ref_with_dot_dot_path) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/schemas/deep/root.json",
          "properties": {
            "types": {
              "$id": "https://example.com/schemas/types.json",
              "definitions": {
                "MyType": { "type": "string" }
              }
            },
            "myField": {
              "$ref": "../foo/../types.json#/definitions/MyType"
            }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    const auto& my_field = schema.root().properties().at("myField");
    ASSERT_EQ(my_field.ref_value(), "../foo/../types.json#/definitions/MyType");
    ASSERT_EQ(my_field.ref()->types(), std::vector{json_value_type::string});
}

TEST(frontend_test, ref_multibase) {
    // Test that a schema reachable via multiple paths (due to nested $id)
    // resolves correctly from both paths.
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "definitions": {
              "Person": {
                  "$id": "person.json",
                  "type": "object",
                  "properties": {
                      "name": { "type": "string" }
                  }
              }
          },
          "properties": {
              "viaRootPath": {
                  "$ref": "#/definitions/Person/properties/name"
              },
              "viaNestedId": {
                  "$ref": "person.json#/properties/name"
              }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    const auto& via_root = schema.root().properties().at("viaRootPath");
    const auto& via_nested = schema.root().properties().at("viaNestedId");

    // Both refs should resolve to the exact same subschema
    ASSERT_EQ(via_root.ref(), via_nested.ref());
    ASSERT_NE(via_root.ref(), nullptr);
}

TEST(frontend_test, ref_infinite_recursion) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#"
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_ref = [](const subschema& s) {
        return std::get<std::reference_wrapper<const subschema>>(s.items())
          .get()
          .ref();
    };

    ASSERT_EQ(items_ref(schema.root()), &schema.root());
    ASSERT_EQ(items_ref(*items_ref(schema.root())), &schema.root());
}

TEST(frontend_test, ref_mutual_recursion) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/root.json",
          "definitions": {
              "A": {
                  "type": "object",
                  "properties": {
                      "toB": { "$ref": "#/definitions/B" }
                  }
              },
              "B": {
                  "type": "object",
                  "properties": {
                      "toA": { "$ref": "#/definitions/A" }
                  }
              }
          },
          "properties": {
              "start": { "$ref": "#/definitions/A" }
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    const auto& def_a = schema.root().subschemas().at("definitions/A");
    const auto& def_b = schema.root().subschemas().at("definitions/B");

    const auto& a_to_b = def_a.properties().at("toB");
    const auto& b_to_a = def_b.properties().at("toA");

    ASSERT_EQ(a_to_b.ref(), &def_b);
    ASSERT_EQ(b_to_a.ref(), &def_a);

    // Verify the cycle: A -> B -> A
    ASSERT_EQ(a_to_b.ref()->properties().at("toA").ref(), &def_a);
}

TEST(frontend_test, ref_invalid_type) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
              "$ref": 123
          }
      })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("Invalid type for keyword $ref. Expected one of: [string].")));
}

TEST(frontend_test, ref_invalid_fragment) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
              "$ref": "#boo"
          }
      })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("$ref #boo is not a valid JSON Pointer")));
}

TEST(frontend_test, ref_to_nonexistent_node) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
              "$ref": "#/definitions/nonexistent"
          },
          "definitions": {
              "inner": { "type": "string" }
          }
      })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(StrEq(
        "$ref #/definitions/nonexistent points to non-existent location")));
}

TEST(frontend_test, ref_to_unknown_keyword) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
              "$ref": "#/foo/bar"
          },
          "foo": {
              "bar": { "type": "string" }
          }
      })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(
        StrEq("$ref #/foo/bar points to uncompiled keyword (not supported)")));
}

TEST(frontend_test, ref_within_unknown_keyword) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
          "$id": "https://example.com/root.json",
          "items": {
            "$ref": "#/customKeyword"
          },
          "customKeyword": {
              "$ref": "#/definitions/inner"
          },
          "definitions": {
              "inner": { "type": "string" }
          }
      })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(StrEq(
        "$ref #/customKeyword points to uncompiled keyword (not supported)")));
}

TEST(frontend_test, ref_root_schema) {
    EXPECT_THAT(
      []() {
          frontend{}.compile(
            parse_json(R"({
              "$ref": "https://other.example.com/schemas/types.json"
            })"),
            "https://example.com/irrelevant-base.json",
            dialect::draft7);
      },
      ThrowsMessage<std::runtime_error>(StrEq(
        "$ref at the root of the schema in draft-07 is not allowed to avoid "
        "undefined behavior")));
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

TEST(frontend_test, id_with_dot_dot_path) {
    auto schema = frontend{}.compile(
      parse_json(R"({
          "$id": "https://example.com/schemas/deep/root.json",
          "items": {
            "$id": "../types.json"
          }
      })"),
      "https://example.com/irrelevant-base.json",
      dialect::draft7);

    auto items_schema = std::get<std::reference_wrapper<const subschema>>(
      schema.root().items());
    ASSERT_EQ(
      items_schema.get().base().id(), "https://example.com/schemas/types.json");
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
          "definitions": {
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

        ASSERT_EQ(expected, ir_tree_printer::to_string(*schema));
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
