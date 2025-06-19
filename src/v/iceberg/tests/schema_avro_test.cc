/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

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
    ASSERT_EQ(std::string_view(parent->nameAt(pos)), expected_name);
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

TEST(SchemaAvroSerialization, TestStructInvalidFieldNames) {
    struct_type type;
    type.fields.emplace_back(nested_field::create(
      0, "42_starts_with_digit", field_required::yes, int_type()));
    type.fields.emplace_back(nested_field::create(
      0, "42.also.has.dots", field_required::yes, int_type()));

    // Note: Encoding this type of characters is inconsistent between Redpanda,
    // Java Iceberg, and Python Iceberg.
    // - Redpanda encodes one char (byte) at a time: `_xF0_x9F_x98_x8E`,
    // - Java encodes one UTF-16 char at a time: `_xD83D_xDE0E`.
    // - Python encodes unicode character at a time: `_x1F60E`.
    type.fields.emplace_back(
      nested_field::create(0, "😎", field_required::yes, int_type()));
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
            "name": "_42_starts_with_digit",
            "type": "int",
            "field-id": 0
        },
        {
            "name": "_42_x2Ealso_x2Ehas_x2Edots",
            "type": "int",
            "field-id": 0
        },
        {
            "name": "_xF0_x9F_x98_x8E",
            "type": "int",
            "field-id": 0
        }
    ]
}
)";
    ASSERT_STREQ(expected_str, avro_schema.toJson().c_str());

    // We don't try to parse it back as these names aren't decoded when
    // deserializing from Avro.
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

TEST(SchemaAvroSerialization, TestLogicalTypes) {
    struct_type type;
    type.fields.emplace_back(nested_field::create(
      0, "decimal", field_required::yes, decimal_type{8, 0}));
    type.fields.emplace_back(
      nested_field::create(1, "date", field_required::yes, date_type{}));
    type.fields.emplace_back(
      nested_field::create(2, "time", field_required::yes, time_type{}));
    type.fields.emplace_back(nested_field::create(
      3, "timestamp", field_required::yes, timestamp_type{}));
    type.fields.emplace_back(
      nested_field::create(4, "uuid", field_required::yes, uuid_type{}));

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
            "name": "decimal",
            "type": {
                "type": "fixed",
                "name": "decimal",
                "size": 4,
                "logicalType": "decimal", "precision": 8, "scale": 0
            },
            "field-id": 0
        },
        {
            "name": "date",
            "type": {
            "type": "int",
            "logicalType": "date"
},
            "field-id": 1
        },
        {
            "name": "time",
            "type": {
            "type": "long",
            "logicalType": "time-micros"
},
            "field-id": 2
        },
        {
            "name": "timestamp",
            "type": {
            "type": "long",
            "logicalType": "timestamp-micros"
},
            "field-id": 3
        },
        {
            "name": "uuid",
            "type": {
            "type": "string",
            "logicalType": "uuid"
},
            "field-id": 4
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
