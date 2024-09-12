// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes.h"
#include "iceberg/manifest_entry_type.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/schema_avro.h"

#include <avro/ValidSchema.hh>
#include <gtest/gtest.h>

using namespace iceberg;

TEST(ManifestEntryTypeTest, TestEmptyPartitionFieldValidSchema) {
    auto entry_type = manifest_entry_type(partition_key_type{});
    auto entry_schema = avro::ValidSchema(
      struct_type_to_avro(entry_type, "manifest_entry"));
    auto expected_partition_field = R"(
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": [
                            ]
                        },
                        "field-id": 102
                    },
)";
    const auto schema_json = entry_schema.toJson(/*prettyPrint=*/true);
    ASSERT_TRUE(schema_json.find(expected_partition_field) != std::string::npos)
      << schema_json;
}

TEST(ManifestEntryTypeTest, TestRequiredPartitionFieldValidSchema) {
    auto test_struct = struct_type{};
    test_struct.fields.emplace_back(
      nested_field::create(1000, "test_pk", field_required::yes, int_type{}));
    auto entry_type = manifest_entry_type(
      partition_key_type{std::move(test_struct)});
    auto entry_schema = avro::ValidSchema(
      struct_type_to_avro(entry_type, "manifest_entry"));
    auto expected_partition_field = R"(
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": [
                                {
                                    "name": "test_pk",
                                    "type": "int",
                                    "field-id": 1000
                                }
                            ]
                        },
                        "field-id": 102
                    },
)";
    const auto schema_json = entry_schema.toJson(/*prettyPrint=*/true);
    ASSERT_TRUE(schema_json.find(expected_partition_field) != std::string::npos)
      << schema_json;
}

TEST(ManifestEntryTypeTest, TestOptionalPartitionFieldValidSchema) {
    auto test_struct = struct_type{};
    test_struct.fields.emplace_back(
      nested_field::create(1000, "test_pk", field_required::no, int_type{}));
    auto entry_type = manifest_entry_type(
      partition_key_type{std::move(test_struct)});
    auto entry_schema = avro::ValidSchema(
      struct_type_to_avro(entry_type, "manifest_entry"));
    auto expected_partition_field = R"(
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": [
                                {
                                    "name": "test_pk",
                                    "type": [
                                        "null",
                                        "int"
                                    ],
                                    "default": null,
                                    "field-id": 1000
                                }
                            ]
                        },
                        "field-id": 102
                    },
)";
    const auto schema_json = entry_schema.toJson(/*prettyPrint=*/true);
    ASSERT_TRUE(schema_json.find(expected_partition_field) != std::string::npos)
      << schema_json;
}
