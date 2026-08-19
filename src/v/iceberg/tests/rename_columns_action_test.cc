/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/datatypes.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/transaction.h"

#include <gtest/gtest.h>

using namespace iceberg;
using namespace iceberg::table_update;
using namespace iceberg::table_requirement;

class RenameColumnsActionTest : public ::testing::Test {
public:
    schema make_schema(int32_t id) {
        auto type = std::get<struct_type>(test_nested_schema_type());
        return schema{
          .schema_struct = std::move(type),
          .schema_id = schema::id_t{id},
          .identifier_field_ids = {},
        };
    }

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

TEST_F(RenameColumnsActionTest, TestRenameTopLevelField) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"foo"}, .new_name = "foo_v1"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_FALSE(res.has_error());

    ASSERT_EQ(tx.updates().updates.size(), 2);
    ASSERT_TRUE(std::holds_alternative<add_schema>(tx.updates().updates[0]));
    ASSERT_TRUE(
      std::holds_alternative<set_current_schema>(tx.updates().updates[1]));

    const auto& added = std::get<add_schema>(tx.updates().updates[0]);
    ASSERT_EQ(added.schema.schema_id(), 1);
    ASSERT_EQ(added.schema.schema_struct.fields[0]->name, "foo_v1");

    ASSERT_FALSE(added.last_column_id.has_value());

    ASSERT_EQ(tx.updates().requirements.size(), 1);
    ASSERT_TRUE(
      std::holds_alternative<assert_current_schema_id>(
        tx.updates().requirements[0]));
    ASSERT_EQ(
      std::get<assert_current_schema_id>(tx.updates().requirements[0])
        .current_schema_id(),
      0);
}

TEST_F(RenameColumnsActionTest, TestRenameNonExistentField) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"nonexistent"}, .new_name = "nope"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), action::errc::unexpected_state);
}

TEST_F(RenameColumnsActionTest, TestEmptyRenamesIsNoop) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_TRUE(tx.is_noop());
}

TEST_F(RenameColumnsActionTest, TestRenameToSameNameIsNoop) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"foo"}, .new_name = "foo"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_TRUE(tx.is_noop());
}

TEST_F(RenameColumnsActionTest, TestMultipleRenames) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"foo"}, .new_name = "foo_v1"});
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"bar"}, .new_name = "bar_v1"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_FALSE(res.has_error());

    const auto& added = std::get<add_schema>(tx.updates().updates[0]);
    ASSERT_EQ(added.schema.schema_struct.fields[0]->name, "foo_v1");
    ASSERT_EQ(added.schema.schema_struct.fields[1]->name, "bar_v1");
}

TEST_F(RenameColumnsActionTest, TestRenameNestedField) {
    // test_nested_schema_type has a "location" struct with "latitude" inside.
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"location", "latitude"}, .new_name = "latitude_v1"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_FALSE(tx.is_noop());
}

TEST_F(RenameColumnsActionTest, TestRenameNonExistentNestedPath) {
    transaction tx(create_table());

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"foo", "deep"}, .new_name = "nope"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_TRUE(res.has_error());
}

TEST_F(RenameColumnsActionTest, TestErroredTransaction) {
    auto tx = transaction::make_with_error(
      create_table(), action::errc::unexpected_state);

    chunked_vector<rename_columns_action::rename_entry> renames;
    renames.emplace_back(
      rename_columns_action::rename_entry{
        .field_path = {"foo"}, .new_name = "foo_v1"});
    auto res = tx.rename_columns(std::move(renames)).get();
    ASSERT_TRUE(res.has_error());
}
