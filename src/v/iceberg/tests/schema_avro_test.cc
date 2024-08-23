// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/schema.h"
#include "iceberg/schema_avro.h"
#include "iceberg/tests/test_schemas.h"

#include <avro/LogicalType.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>
#include <gtest/gtest.h>

using namespace iceberg;

namespace {

void check_name(
  const avro::NodePtr& parent, size_t pos, std::string_view expected_name) {
    ASSERT_STREQ(parent->nameAt(pos).c_str(), expected_name.cbegin());
}

void check_optional(const avro::NodePtr& n, avro::Type expected_type) {
    ASSERT_EQ(n->type(), avro::Type::AVRO_UNION);
    ASSERT_EQ(n->leafAt(0)->type(), avro::Type::AVRO_NULL);
    ASSERT_EQ(n->leafAt(1)->type(), expected_type);
}

void check_map(
  const avro::NodePtr& n, avro::Type expected_key, avro::Type expected_val) {
    ASSERT_EQ(n->type(), avro::Type::AVRO_ARRAY);
    ASSERT_EQ(n->leaves(), 1);
    ASSERT_EQ(n->leafAt(0)->leaves(), 2);
    ASSERT_EQ(n->leafAt(0)->leafAt(0)->type(), expected_key);
    ASSERT_EQ(n->leafAt(0)->leafAt(1)->type(), expected_val);
}

void check_list(const avro::NodePtr& n, avro::Type expected_type) {
    ASSERT_EQ(n->type(), avro::Type::AVRO_ARRAY);
    ASSERT_EQ(n->leafAt(0)->type(), expected_type);
}

} // namespace

TEST(SchemaAvroSerialization, TestSimpleFields) {
    struct_type type;
    type.fields.emplace_back(
      nested_field::create(0, "foo", field_required::yes, int_type()));
    type.fields.emplace_back(
      nested_field::create(1, "bar", field_required::no, int_type()));
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    auto avro_root = struct_type_to_avro(s.schema_struct, "test_schema");
    auto avro_schema = avro::ValidSchema(avro_root);
    const auto expected_str = R"({
    "type": "record",
    "name": "test_schema",
    "fields": [
        {
            "name": "foo",
            "type": "int",
            "field-id": 0
        },
        {
            "name": "bar",
            "type": [
                "null",
                "int"
            ],
            "default": null,
            "field-id": 1
        }
    ]
}
)";
    ASSERT_STREQ(expected_str, avro_schema.toJson().c_str());

    // Now parse it back from Avro and ensure the types are equal.
    auto parsed_type = type_from_avro(avro_schema.root());
    ASSERT_TRUE(std::holds_alternative<struct_type>(parsed_type));
    const auto& parsed_struct = std::get<struct_type>(parsed_type);
    ASSERT_EQ(s.schema_struct, parsed_struct);
}

TEST(SchemaAvroSerialization, TestListFields) {
    struct_type type;
    type.fields.emplace_back(nested_field::create(
      0,
      "foo",
      field_required::no,
      list_type::create(1, field_required::yes, int_type())));
    type.fields.emplace_back(nested_field::create(
      2,
      "bar",
      field_required::yes,
      list_type::create(
        3,
        field_required::yes,
        map_type::create(
          4, int_type(), 5, field_required::no, string_type()))));
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    auto avro_root = struct_type_to_avro(s.schema_struct, "test_schema");
    auto avro_schema = avro::ValidSchema(avro_root);
    const auto expected_str = R"({
    "type": "record",
    "name": "test_schema",
    "fields": [
        {
            "name": "foo",
            "type": [
                "null",
                {
                    "type": "array",
                    "element-id": 1,
                    "items": "int"
                }
            ],
            "default": null,
            "field-id": 0
        },
        {
            "name": "bar",
            "type": {
                "type": "array",
                "element-id": 3,
                "items": {
                    "type": "array",
                "logicalType": "map",
                    "items": {
                        "type": "record",
                        "name": "k4_v5",
                        "fields": [
                            {
                                "name": "key",
                                "type": "int",
                                "field-id": 4
                            },
                            {
                                "name": "value",
                                "type": [
                                    "null",
                                    "string"
                                ],
                                "field-id": 5
                            }
                        ]
                    }
                }
            },
            "field-id": 2
        }
    ]
}
)";
    ASSERT_STREQ(expected_str, avro_schema.toJson().c_str());

    // Now parse it back from Avro and ensure the types are equal.
    auto parsed_type = type_from_avro(avro_schema.root());
    ASSERT_TRUE(std::holds_alternative<struct_type>(parsed_type));
    const auto& parsed_struct = std::get<struct_type>(parsed_type);
    ASSERT_EQ(s.schema_struct, parsed_struct);
}

TEST(SchemaAvroSerialization, TestStruct) {
    struct_type type;
    type.fields.emplace_back(
      nested_field::create(0, "simpleint", field_required::yes, int_type()));
    struct_type subtype;
    subtype.fields.emplace_back(
      nested_field::create(1, "foo", field_required::yes, int_type()));
    subtype.fields.emplace_back(
      nested_field::create(2, "bar", field_required::no, int_type()));
    type.fields.emplace_back(nested_field::create(
      3, "substruct", field_required::no, std::move(subtype)));
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    auto avro_root = struct_type_to_avro(s.schema_struct, "test_schema");
    auto avro_schema = avro::ValidSchema(avro_root);
    const auto expected_str = R"({
    "type": "record",
    "name": "test_schema",
    "fields": [
        {
            "name": "simpleint",
            "type": "int",
            "field-id": 0
        },
        {
            "name": "substruct",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "r3",
                    "fields": [
                        {
                            "name": "foo",
                            "type": "int",
                            "field-id": 1
                        },
                        {
                            "name": "bar",
                            "type": [
                                "null",
                                "int"
                            ],
                            "default": null,
                            "field-id": 2
                        }
                    ]
                }
            ],
            "default": null,
            "field-id": 3
        }
    ]
}
)";
    ASSERT_STREQ(expected_str, avro_schema.toJson().c_str());

    // Now parse it back from Avro and ensure the types are equal.
    auto parsed_type = type_from_avro(avro_schema.root());
    ASSERT_TRUE(std::holds_alternative<struct_type>(parsed_type));
    const auto& parsed_struct = std::get<struct_type>(parsed_type);
    ASSERT_EQ(s.schema_struct, parsed_struct);
}

TEST(SchemaAvroSerialization, TestMap) {
    struct_type type;
    type.fields.emplace_back(nested_field::create(
      0,
      "intmap",
      field_required::yes,
      map_type::create(1, int_type(), 2, field_required::no, int_type())));
    type.fields.emplace_back(nested_field::create(
      3,
      "listmap",
      field_required::yes,
      map_type::create(
        4,
        list_type::create(5, field_required::no, int_type()),
        6,
        field_required::no,
        string_type())));
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    auto avro_root = struct_type_to_avro(s.schema_struct, "test_schema");
    auto avro_schema = avro::ValidSchema(avro_root);
    const auto expected_str = R"({
    "type": "record",
    "name": "test_schema",
    "fields": [
        {
            "name": "intmap",
            "type": {
                "type": "array",
            "logicalType": "map",
                "items": {
                    "type": "record",
                    "name": "k1_v2",
                    "fields": [
                        {
                            "name": "key",
                            "type": "int",
                            "field-id": 1
                        },
                        {
                            "name": "value",
                            "type": [
                                "null",
                                "int"
                            ],
                            "field-id": 2
                        }
                    ]
                }
            },
            "field-id": 0
        },
        {
            "name": "listmap",
            "type": {
                "type": "array",
            "logicalType": "map",
                "items": {
                    "type": "record",
                    "name": "k4_v6",
                    "fields": [
                        {
                            "name": "key",
                            "type": {
                                "type": "array",
                                "element-id": 5,
                                "items": [
                                    "null",
                                    "int"
                                ]
                            },
                            "field-id": 4
                        },
                        {
                            "name": "value",
                            "type": [
                                "null",
                                "string"
                            ],
                            "field-id": 6
                        }
                    ]
                }
            },
            "field-id": 3
        }
    ]
}
)";
    ASSERT_STREQ(expected_str, avro_schema.toJson().c_str());

    // Now parse it back from Avro and ensure the types are equal.
    auto parsed_type = type_from_avro(avro_schema.root());
    ASSERT_TRUE(std::holds_alternative<struct_type>(parsed_type));
    const auto& parsed_struct = std::get<struct_type>(parsed_type);
    ASSERT_EQ(s.schema_struct, parsed_struct);
}

TEST(SchemaAvroSerialization, TestNestedSchema) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    auto avro_node = struct_type_to_avro(s.schema_struct, "test_schema");
    ASSERT_EQ(avro_node.type(), avro::Type::AVRO_RECORD);
    const auto& root = avro_node.root();
    ASSERT_EQ(root->leaves(), 7);

    ASSERT_NO_FATAL_FAILURE(check_name(root, 0, "foo"));
    const auto& leaf0 = avro_node.root()->leafAt(0);
    ASSERT_NO_FATAL_FAILURE(check_optional(leaf0, avro::Type::AVRO_STRING));

    ASSERT_NO_FATAL_FAILURE(check_name(root, 1, "bar"));
    const auto& leaf1 = avro_node.root()->leafAt(1);
    ASSERT_EQ(leaf1->type(), avro::Type::AVRO_INT);

    ASSERT_NO_FATAL_FAILURE(check_name(root, 2, "baz"));
    const auto& leaf2 = avro_node.root()->leafAt(2);
    ASSERT_NO_FATAL_FAILURE(check_optional(leaf2, avro::Type::AVRO_BOOL));

    ASSERT_NO_FATAL_FAILURE(check_name(root, 3, "qux"));
    const auto& leaf3 = avro_node.root()->leafAt(3);
    ASSERT_EQ(leaf3->type(), avro::Type::AVRO_ARRAY);
    ASSERT_EQ(leaf3->leafAt(0)->type(), avro::Type::AVRO_STRING);

    ASSERT_NO_FATAL_FAILURE(check_name(root, 4, "quux"));
    const auto& leaf4 = avro_node.root()->leafAt(4);
    ASSERT_NO_FATAL_FAILURE(
      check_map(leaf4, avro::Type::AVRO_STRING, avro::Type::AVRO_ARRAY));
    ASSERT_NO_FATAL_FAILURE(check_map(
      leaf4->leafAt(0)->leafAt(1),
      avro::Type::AVRO_STRING,
      avro::Type::AVRO_INT));

    ASSERT_NO_FATAL_FAILURE(check_name(root, 5, "location"));
    const auto& leaf5 = avro_node.root()->leafAt(5);
    ASSERT_NO_FATAL_FAILURE(check_list(leaf5, avro::Type::AVRO_RECORD));
    ASSERT_EQ(leaf5->leafAt(0)->leaves(), 2);
    ASSERT_NO_FATAL_FAILURE(check_name(leaf5->leafAt(0), 0, "latitude"));
    ASSERT_NO_FATAL_FAILURE(check_name(leaf5->leafAt(0), 1, "longitude"));
    ASSERT_NO_FATAL_FAILURE(
      check_optional(leaf5->leafAt(0)->leafAt(0), avro::Type::AVRO_FLOAT));
    ASSERT_NO_FATAL_FAILURE(
      check_optional(leaf5->leafAt(0)->leafAt(1), avro::Type::AVRO_FLOAT));

    ASSERT_NO_FATAL_FAILURE(check_name(root, 6, "person"));
    const auto& leaf6 = avro_node.root()->leafAt(6);
    ASSERT_NO_FATAL_FAILURE(check_optional(leaf6, avro::Type::AVRO_RECORD));
    ASSERT_TRUE(leaf6->leafAt(1)->hasName());
    ASSERT_EQ(leaf6->leafAt(1)->leafAt(0)->leaves(), 2);
    ASSERT_NO_FATAL_FAILURE(check_name(leaf6->leafAt(1), 0, "name"));
    ASSERT_NO_FATAL_FAILURE(check_name(leaf6->leafAt(1), 1, "age"));
    ASSERT_NO_FATAL_FAILURE(
      check_optional(leaf6->leafAt(1)->leafAt(0), avro::Type::AVRO_STRING));
    ASSERT_EQ(leaf6->leafAt(1)->leafAt(1)->type(), avro::Type::AVRO_INT);

    // Now parse it back from Avro and ensure the types are equal.
    auto parsed_type = type_from_avro(avro_node.root());
    ASSERT_TRUE(std::holds_alternative<struct_type>(parsed_type));
    const auto& parsed_struct = std::get<struct_type>(parsed_type);
    ASSERT_EQ(s.schema_struct, parsed_struct);
}
