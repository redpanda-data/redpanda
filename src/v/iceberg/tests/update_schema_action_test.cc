// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/tests/test_schemas.h"
#include "iceberg/transaction.h"

#include <gtest/gtest.h>

using namespace iceberg;
using namespace iceberg::table_update;
using namespace iceberg::table_requirement;

class UpdateSchemaActionTest : public ::testing::Test {
public:
    // Create a nested schema, adding columns as requested.
    schema make_schema(int32_t id, size_t extra_cols = 0) {
        constexpr nested_field::id_t base_field_id{50};
        auto type = std::get<struct_type>(test_nested_schema_type());
        for (size_t i = 0; i < extra_cols; ++i) {
            type.fields.emplace_back(nested_field::create(
              base_field_id() + i,
              fmt::format("test{}", i),
              field_required::no,
              boolean_type{}));
        }
        return schema{
          .schema_struct = std::move(type),
          .schema_id = schema::id_t{id},
          .identifier_field_ids = {},
        };
    }
    // Create a simple table with no snapshots.
    table_metadata create_table() {
        auto s = make_schema(0);
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());
        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = uri("s3://foo/bar"),
          .last_sequence_number = sequence_number{0},
          .last_updated_ms = model::timestamp::now(),
          .last_column_id = s.highest_field_id().value(),
          .schemas = std::move(schemas),
          .current_schema_id = schema::id_t{0},
          .partition_specs = {},
          .default_spec_id = partition_spec::id_t{0},
          .last_partition_id = partition_field::id_t{-1},
        };
    }
};

TEST_F(UpdateSchemaActionTest, TestExistingCurrentSchema) {
    transaction tx(create_table());

    // Point to a new schema id that has the same type. The id is ignored and
    // the type is used to identify the schema.
    auto res = tx.set_schema(make_schema(12345)).get();
    // The schema has an identical type to the current schema. It should no-op.
    ASSERT_FALSE(res.has_error());
    ASSERT_EQ(tx.table().current_schema_id(), 0);
    ASSERT_TRUE(tx.updates().updates.empty());
    ASSERT_TRUE(tx.updates().requirements.empty());
}

TEST_F(UpdateSchemaActionTest, TestExistingSchema) {
    // Setup: point the current schema id at a new schema with id 1.
    auto table = create_table();
    table.schemas.emplace_back(make_schema(1, 1));
    table.current_schema_id = schema::id_t{1};

    // Update to the same schema as schema_id 0.
    transaction tx(std::move(table));
    ASSERT_EQ(tx.table().schemas.size(), 2);
    auto res = tx.set_schema(make_schema(12345)).get();

    // The action should point our table to schema_id 0.
    ASSERT_FALSE(res.has_error());
    ASSERT_EQ(tx.table().current_schema_id(), 0);
    ASSERT_EQ(tx.table().schemas.size(), 2);
    ASSERT_EQ(tx.updates().updates.size(), 1);
    ASSERT_TRUE(
      std::holds_alternative<set_current_schema>(tx.updates().updates[0]));

    // The update should point at schema_id 0.
    ASSERT_EQ(
      std::get<set_current_schema>(tx.updates().updates[0]).schema_id(), 0);
    ASSERT_EQ(tx.updates().requirements.size(), 1);

    // We should require the original schema id.
    ASSERT_TRUE(std::holds_alternative<assert_current_schema_id>(
      tx.updates().requirements[0]));
    ASSERT_EQ(
      std::get<assert_current_schema_id>(tx.updates().requirements[0])
        .current_schema_id(),
      1);
}

TEST_F(UpdateSchemaActionTest, TestNewSchema) {
    transaction tx(create_table());
    auto new_schema = make_schema(12345, 1);
    auto new_schema_len = new_schema.schema_struct.fields.size();
    auto res = tx.set_schema(std::move(new_schema)).get();
    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(tx.table().current_schema_id(), 1);
    ASSERT_EQ(tx.table().schemas.size(), 2);
    ASSERT_EQ(tx.updates().updates.size(), 2);
    ASSERT_TRUE(std::holds_alternative<add_schema>(tx.updates().updates[0]));
    ASSERT_TRUE(
      std::holds_alternative<set_current_schema>(tx.updates().updates[1]));

    // The update should include the new schema.
    const auto& add_update = std::get<add_schema>(tx.updates().updates[0]);
    ASSERT_EQ(add_update.schema.schema_struct.fields.size(), new_schema_len);
    ASSERT_EQ(add_update.schema.schema_id, 1);

    // The update should point at the unassigned id -1.
    ASSERT_EQ(
      std::get<set_current_schema>(tx.updates().updates[1]).schema_id(), -1);

    // We should require the original schema id.
    ASSERT_EQ(tx.updates().requirements.size(), 1);
    ASSERT_TRUE(std::holds_alternative<assert_current_schema_id>(
      tx.updates().requirements[0]));
    ASSERT_EQ(
      std::get<assert_current_schema_id>(tx.updates().requirements[0])
        .current_schema_id(),
      0);

    // Now add another schema within the same transaction.
    // We should double the number of updates, but the requirements should be
    // deduplicated, and the first requirement from above should remain.
    auto new_new_schema = make_schema(12345, 2);
    res = tx.set_schema(std::move(new_new_schema)).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_EQ(tx.updates().updates.size(), 4);
    ASSERT_EQ(tx.updates().requirements.size(), 1);
    ASSERT_TRUE(std::holds_alternative<assert_current_schema_id>(
      tx.updates().requirements[0]));
    ASSERT_EQ(
      std::get<assert_current_schema_id>(tx.updates().requirements[0])
        .current_schema_id(),
      0);
}

TEST_F(UpdateSchemaActionTest, TestNewMultipleSchemas) {
    transaction tx(create_table());
    auto new_schema = make_schema(12345, 1);
    auto new_schema_len = new_schema.schema_struct.fields.size();
    auto res = tx.set_schema(std::move(new_schema)).get();
    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(tx.table().current_schema_id(), 1);
    ASSERT_EQ(tx.table().schemas.size(), 2);
    ASSERT_EQ(tx.updates().updates.size(), 2);
    ASSERT_TRUE(std::holds_alternative<add_schema>(tx.updates().updates[0]));
    ASSERT_TRUE(
      std::holds_alternative<set_current_schema>(tx.updates().updates[1]));

    // The update should include the new schema.
    const auto& add_update = std::get<add_schema>(tx.updates().updates[0]);
    ASSERT_EQ(add_update.schema.schema_struct.fields.size(), new_schema_len);
    ASSERT_EQ(add_update.schema.schema_id, 1);

    // The update should point at the unassigned id -1.
    ASSERT_EQ(
      std::get<set_current_schema>(tx.updates().updates[1]).schema_id(), -1);

    // We should require the original schema id.
    ASSERT_EQ(tx.updates().requirements.size(), 1);
    ASSERT_TRUE(std::holds_alternative<assert_current_schema_id>(
      tx.updates().requirements[0]));
    ASSERT_EQ(
      std::get<assert_current_schema_id>(tx.updates().requirements[0])
        .current_schema_id(),
      0);
}

TEST_F(UpdateSchemaActionTest, TestInvalidSchema) {
    // Removing columns is not yet supported.
    transaction tx(create_table());
    auto new_schema = make_schema(12345);
    new_schema.schema_struct.fields.pop_back();

    auto res = tx.set_schema(std::move(new_schema)).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_TRUE(tx.error().has_value());

    // Adding a new schema after attempting a bad update also fails.
    auto new_new_schema = make_schema(12345, 1);
    res = tx.set_schema(std::move(new_new_schema)).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_TRUE(tx.error().has_value());
}

TEST_F(UpdateSchemaActionTest, TestAssignFieldIds) {
    transaction tx(create_table());
    auto new_schema = make_schema(12345, 10);
    auto new_schema_len = new_schema.schema_struct.fields.size();
    auto res = tx.set_schema(std::move(new_schema)).get();
    ASSERT_FALSE(res.has_error());

    const auto& add_update = std::get<add_schema>(tx.updates().updates[0]);
    ASSERT_EQ(add_update.schema.schema_struct.fields.size(), new_schema_len);

    auto highest_field = add_update.schema.highest_field_id();
    ASSERT_TRUE(highest_field.has_value());
    ASSERT_EQ(27, highest_field.value()());
}
