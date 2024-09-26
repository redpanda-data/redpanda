// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/table_update_applier.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace iceberg;

class UpdateApplyingVisitorTest : public ::testing::Test {
public:
    // Create a simple table with no snapshots.
    table_metadata create_table() {
        auto s = schema{
          .schema_struct = std::get<struct_type>(test_nested_schema_type()),
          .schema_id = schema::id_t{0},
          .identifier_field_ids = {},
        };
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());
        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = "foo/bar",
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

TEST_F(UpdateApplyingVisitorTest, TestAddSchema) {
    const auto make_update =
      [&](
        int32_t schema_id,
        std::optional<int32_t> last_column_id) -> table_update::update {
        return table_update::add_schema{
          .schema = schema{
            .schema_struct = std::get<struct_type>(test_nested_schema_type()),
            .schema_id = schema::id_t{schema_id},
            .identifier_field_ids = {},
          },
          .last_column_id = last_column_id
            ? std::make_optional<nested_field::id_t>(*last_column_id)
            : std::nullopt,
        };
    };
    auto table = create_table();
    auto outcome = table_update::apply(make_update(1, 100), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.current_schema_id(), 0);
    ASSERT_EQ(table.last_column_id(), 100);
    ASSERT_EQ(table.schemas.size(), 2);
    ASSERT_EQ(table.schemas.back().schema_id(), 1);

    // Since the last column id wasn't set in our request, it is not changed.
    outcome = table_update::apply(make_update(2, std::nullopt), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.last_column_id(), 100);
    ASSERT_EQ(table.schemas.size(), 3);
    ASSERT_EQ(table.schemas.back().schema_id(), 2);

    // Last column id should only goes up.
    outcome = table_update::apply(make_update(3, 90), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);

    // Can't add a schema whose id already exists.
    outcome = table_update::apply(make_update(0, 90), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
}

TEST_F(UpdateApplyingVisitorTest, TestSetCurrentSchema) {
    const auto make_update = [&](int32_t schema_id) -> table_update::update {
        return table_update::set_current_schema{
          .schema_id = schema::id_t{schema_id},
        };
    };
    auto table = create_table();

    // Sanity check for a no-op when setting to the current schema.
    ASSERT_EQ(table.current_schema_id(), 0);
    auto outcome = table_update::apply(make_update(0), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.current_schema_id(), 0);

    // Now point to a different schema.
    table.schemas.emplace_back(schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{1},
      .identifier_field_ids = {},
    });
    outcome = table_update::apply(make_update(1), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.current_schema_id(), 1);

    // Pointing at a schema that doesn't exist should fail.
    outcome = table_update::apply(make_update(2), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
    ASSERT_EQ(table.current_schema_id(), 1);
}

TEST_F(UpdateApplyingVisitorTest, TestSetCurrentSchemaUnassigned) {
    const auto make_update = [&](int32_t schema_id) -> table_update::update {
        return table_update::set_current_schema{
          .schema_id = schema::id_t{schema_id},
        };
    };
    auto table = create_table();
    table.schemas.emplace_back(schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{1},
      .identifier_field_ids = {},
    });

    // Setting -1 for the schema is a special value that tells us to use the
    // highest id.
    auto outcome = table_update::apply(make_update(-1), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.current_schema_id(), 1);

    // -1 is only allowed if there are schemas.
    table.schemas.clear();
    outcome = table_update::apply(make_update(-1), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
}

TEST_F(UpdateApplyingVisitorTest, TestAddSpec) {
    auto table = create_table();
    const auto make_update = [&](int32_t spec_id) -> table_update::update {
        return table_update::add_spec{
            .spec = partition_spec{
                .spec_id = partition_spec::id_t{spec_id},
                .fields = {},
            },
        };
    };
    auto outcome = table_update::apply(make_update(0), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.partition_specs.size(), 1);

    outcome = table_update::apply(make_update(1), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.partition_specs.size(), 2);

    // Adding an existing spec fails.
    outcome = table_update::apply(make_update(1), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
}

TEST_F(UpdateApplyingVisitorTest, TestAddSnapshot) {
    const auto make_update = [&](
                               int64_t snap_id,
                               std::optional<int64_t> parent,
                               int32_t seq_num) -> table_update::update {
        return table_update::add_snapshot{
          .snapshot = snapshot{
            .id = snapshot_id{snap_id},
            .parent_snapshot_id = parent
              ? std::make_optional<snapshot_id>(*parent)
              : std::nullopt,
            .sequence_number = sequence_number{seq_num},
          },
        };
    };
    {
        auto table = create_table();
        auto outcome = table_update::apply(
          make_update(12345, std::nullopt, 1), table);
        ASSERT_EQ(outcome, table_update::outcome::success);
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_EQ(table.snapshots->size(), 1);
        ASSERT_EQ(table.last_sequence_number(), 1);

        outcome = table_update::apply(
          make_update(54321, std::nullopt, 2), table);
        ASSERT_EQ(outcome, table_update::outcome::success);
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_EQ(table.snapshots->size(), 2);
        ASSERT_EQ(table.last_sequence_number(), 2);

        // Adding a snapshot that already exists should fail.
        outcome = table_update::apply(
          make_update(12345, std::nullopt, 1), table);
        ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
    }
    {
        auto table = create_table();
        auto outcome = table_update::apply(
          make_update(12345, std::nullopt, 1), table);
        ASSERT_EQ(outcome, table_update::outcome::success);
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_EQ(table.snapshots->size(), 1);
        ASSERT_EQ(table.last_sequence_number(), 1);

        // Sequence number should not decrease.
        outcome = table_update::apply(
          make_update(54321, std::nullopt, 0), table);
        ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
    }
}

TEST_F(UpdateApplyingVisitorTest, TestRemoveSnapshots) {
    const auto make_update = [&](chunked_vector<int64_t> input) {
        chunked_vector<snapshot_id> snaps;
        std::transform(
          input.begin(),
          input.end(),
          std::back_inserter(snaps),
          [](int32_t id) { return snapshot_id{id}; });
        return table_update::remove_snapshots{
          .snapshot_ids = std::move(snaps),
        };
    };
    auto table = create_table();

    // It's not problematic to remove a snapshot that doesn't exist (may have
    // already been removed).
    auto outcome = table_update::apply(make_update({0}), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_FALSE(table.snapshots.has_value());

    // Now actually remove some snapshots.
    table.snapshots.emplace();
    for (int64_t i = 0; i < 100; ++i) {
        table.snapshots->emplace_back(snapshot{
          .id = snapshot_id{i},
        });
    }
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots->size(), 100);
    outcome = table_update::apply(make_update({0, 1, 2, 3}), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.snapshots->size(), 96);
    ASSERT_EQ(table.snapshots->front().id(), 4);
}

TEST_F(UpdateApplyingVisitorTest, TestSetSnapshotReference) {
    const auto make_update =
      [&](std::string_view ref_name, int64_t snap_id) -> table_update::update {
        return table_update::set_snapshot_ref{
            .ref_name = ss::sstring(ref_name),
            .ref = snapshot_reference{
                .snapshot_id = snapshot_id{snap_id},
            },
        };
    };
    auto table = create_table();
    table.snapshots.emplace();
    for (int64_t i = 0; i < 100; ++i) {
        table.snapshots->emplace_back(snapshot{
          .id = snapshot_id{i},
        });
    }
    // Set the table up with some snapshots but don't set a current snapshot.
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_FALSE(table.current_snapshot_id.has_value());
    ASSERT_FALSE(table.refs.has_value());
    ASSERT_EQ(table.snapshots->size(), 100);

    // Add a tag that isn't 'main'.
    auto outcome = table_update::apply(make_update("tag1", 99), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_FALSE(table.current_snapshot_id.has_value());
    ASSERT_TRUE(table.refs.has_value());
    ASSERT_EQ(table.refs->size(), 1);
    ASSERT_TRUE(table.refs->contains("tag1"));

    // Add a 'main' tag and check that the current snapshot gets set.
    outcome = table_update::apply(make_update("main", 99), table);
    ASSERT_EQ(outcome, table_update::outcome::success);
    ASSERT_EQ(table.refs->size(), 2);
    ASSERT_TRUE(table.refs->contains("main"));
    ASSERT_TRUE(table.current_snapshot_id.has_value());
    ASSERT_EQ(table.current_snapshot_id.value()(), 99);

    // Trying to add a reference that doesn't exist fails.
    outcome = table_update::apply(make_update("noneya", 12345), table);
    ASSERT_EQ(outcome, table_update::outcome::unexpected_state);
}
