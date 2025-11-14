/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vassert.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/remove_snapshots_action.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "random/generators.h"

#include <gtest/gtest.h>

using namespace iceberg;
using namespace iceberg::table_update;
using namespace std::chrono_literals;

namespace {

// Creates a dummy table metadata.
table_metadata create_table() {
    return table_metadata{
      .format_version = format_version::v2,
      .table_uuid = uuid_t::create(),
      .location = uri("s3://foo/bar"),
      .last_sequence_number = sequence_number{0},
      .last_updated_ms = model::timestamp::now(),
      .last_column_id = nested_field::id_t{-1},
      .schemas = {},
      .current_schema_id = schema::id_t{-1},
      .partition_specs = {},
      .default_spec_id = partition_spec::id_t{0},
      .last_partition_id = partition_field::id_t{-1},
    };
}

struct snapshot_removal {
    chunked_vector<snapshot_id> snaps;
    chunked_vector<ss::sstring> refs;
};
// Computes the set of snapshots and references to remove if run at the given
// timestamp.
snapshot_removal
compute_removal(const table_metadata& table, model::timestamp::type now) {
    snapshot_removal removal_res;
    remove_snapshots_action action(table, model::timestamp{now});
    action.compute_removed_snapshots(&removal_res.snaps, &removal_res.refs);
    return removal_res;
}

snapshot_reference& add_branch(
  snapshot_id id, const ss::sstring& branch_name, table_metadata* table) {
    if (!table->refs.has_value()) {
        table->refs.emplace();
    }
    table->refs->emplace(
      branch_name,
      snapshot_reference{
        .snapshot_id = id,
        .type = snapshot_ref_type::branch,
      });
    return table->refs->at(branch_name);
}
snapshot_reference&
add_tag(snapshot_id id, const ss::sstring& tag_name, table_metadata* table) {
    if (!table->refs.has_value()) {
        table->refs.emplace();
    }
    table->refs->emplace(
      tag_name,
      snapshot_reference{
        .snapshot_id = id,
        .type = snapshot_ref_type::branch,
      });
    return table->refs->at(tag_name);
}

// Examines the updates from the transaction and checks that they match the
// expected counts, also ensuring that snapshot removals are stored
// individually, to match the expectations from Java implementations.
// https://github.com/apache/iceberg/blob/3e6da2e5437ffb3f643275927e5580cb9620256b/core/src/main/java/org/apache/iceberg/MetadataUpdateParser.java#L550-L553
void check_expected_updates(
  const transaction& txn,
  size_t expected_snap_removals,
  size_t expected_ref_removals) {
    size_t snapshot_removals = 0;
    size_t ref_removals = 0;
    for (const auto& u : txn.updates().updates) {
        if (std::holds_alternative<remove_snapshots>(u)) {
            auto& r = std::get<remove_snapshots>(u);
            EXPECT_EQ(1, r.snapshot_ids.size());
            snapshot_removals++;
        } else if (std::holds_alternative<remove_snapshot_ref>(u)) {
            ref_removals++;
        } else {
            FAIL() << "Unexpected update: " << u.index();
        }
    }
    EXPECT_EQ(expected_snap_removals, snapshot_removals);
    EXPECT_EQ(ref_removals, expected_ref_removals);
}

static constexpr size_t default_total_snaps = 100;

} // namespace

class RemoveSnapshotsActionTest : public ::testing::Test {
public:
    // Adds the given number of snapshots to the table. Returns the last
    // snapshot ID used.
    snapshot_id add_snapshots(
      size_t num_snapshots,
      model::timestamp::type timestamp,
      table_metadata* table) {
        vassert(num_snapshots > 0, "Need positive num_snapshots");
        if (!table->snapshots.has_value()) {
            table->snapshots.emplace();
        }
        auto& table_snaps = table->snapshots.value();
        std::optional<snapshot_id> parent;
        for (size_t i = 0; i < num_snapshots; ++i) {
            const auto id = use_next_snap_id();
            table_snaps.emplace_back(
              snapshot{
                .id = id,
                .parent_snapshot_id = parent,
                .timestamp_ms = model::timestamp{timestamp},
                .summary = {.operation = snapshot_operation::append},
                .manifest_list_path = uri(fmt::format("s3://snap-{}", id())),
                .schema_id = std::nullopt,
              });
            parent = id;
        }
        return table_snaps.back().id;
    }

private:
    // Counter for new snapshot IDs to ensure uniqueness.
    // NOTE: this is not represent any kind of ordering!
    snapshot_id use_next_snap_id() { return next_snap_id_++; }

    snapshot_id next_snap_id_{0};
};

TEST_F(RemoveSnapshotsActionTest, TestRetainDefaultsMain) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    auto table = create_table();
    auto highest_id = add_snapshots(default_total_snaps, now.value(), &table);

    // Run the removal such that we're right at the border of retention.
    // Nothing should be removed.
    auto retained_removal = compute_removal(table, now.value() + expiry_age_ms);
    ASSERT_TRUE(retained_removal.snaps.empty());
    ASSERT_TRUE(retained_removal.refs.empty());

    // When we run removal a bit later, our snapshots should all become
    // eligible for removal, but we should retain the minimum.
    {
        // The current snapshot is treated as the tip of the main branch if not
        // explicitly referenced.
        table.current_snapshot_id = highest_id;
        auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
        ASSERT_EQ(
          removal.snaps.size(),
          default_total_snaps
            - remove_snapshots_action::default_min_snapshots_retained);
        ASSERT_TRUE(removal.refs.empty());
    }
    {
        table.current_snapshot_id = std::nullopt;
        add_branch(highest_id, "main", &table);
        auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
        ASSERT_EQ(
          removal.snaps.size(),
          default_total_snaps
            - remove_snapshots_action::default_min_snapshots_retained);
        ASSERT_TRUE(removal.refs.empty());
    }
}

TEST_F(RemoveSnapshotsActionTest, TestRetainDefaultsMissingMain) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    auto table = create_table();
    add_snapshots(default_total_snaps, now.value(), &table);

    // NOTE: This is the default, but we're explicitly setting for visibility
    // in this test. This isn't an expected state for Iceberg -- we expect the
    // current snapshot to be set. Testing for the sake of completeness.
    table.current_snapshot_id = std::nullopt;

    // When we run removal, our snapshots should all become eligible for
    // removal. Since we don't have a current snapshot set or a 'main' branch,
    // all of the snapshots get removed without keeping any minimum number.
    // This matches behavior in Apache Iceberg.
    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    ASSERT_EQ(removal.snaps.size(), default_total_snaps);
    ASSERT_TRUE(removal.refs.empty());
}

TEST_F(RemoveSnapshotsActionTest, TestRetainMainByTime) {
    const auto now = model::timestamp::now();
    const auto custom_max_snapshot_age_ms = 42;
    auto table = create_table();
    auto highest_id = add_snapshots(default_total_snaps, now.value(), &table);
    auto& main_ref = add_branch(highest_id, "main", &table);
    main_ref.max_snapshot_age_ms = custom_max_snapshot_age_ms;

    auto retained_removal = compute_removal(
      table, now.value() + custom_max_snapshot_age_ms);
    ASSERT_TRUE(retained_removal.snaps.empty());
    ASSERT_TRUE(retained_removal.refs.empty());

    auto removal = compute_removal(
      table, now.value() + custom_max_snapshot_age_ms + 1);
    ASSERT_EQ(
      removal.snaps.size(),
      default_total_snaps
        - remove_snapshots_action::default_min_snapshots_retained);
    ASSERT_TRUE(removal.refs.empty());
}

TEST_F(RemoveSnapshotsActionTest, TestRetainMainByCount) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    auto table = create_table();
    auto highest_id = add_snapshots(default_total_snaps, now.value(), &table);
    auto& main_ref = add_branch(highest_id, "main", &table);

    // Set some number of snapshots to keep in this branch.
    const auto custom_min_to_keep = 42;
    main_ref.min_snapshots_to_keep = custom_min_to_keep;
    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    ASSERT_EQ(removal.snaps.size(), default_total_snaps - custom_min_to_keep);
    ASSERT_TRUE(removal.refs.empty());
}

TEST_F(RemoveSnapshotsActionTest, TestRemoveUnreferenced) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    auto table = create_table();
    auto id1 = add_snapshots(1, now.value(), &table);
    add_tag(id1, "tag1", &table);
    auto id2 = add_snapshots(1, now.value(), &table);
    add_tag(id2, "tag2", &table);

    const auto unreferenced_count = 42;
    for (int i = 0; i < unreferenced_count; ++i) {
        add_snapshots(1, now.value(), &table);
    }
    auto retained_removal = compute_removal(table, now.value() + expiry_age_ms);
    ASSERT_TRUE(retained_removal.snaps.empty());
    ASSERT_TRUE(retained_removal.refs.empty());

    // All the unreferenced snapshots should be removed, while we keep the
    // tagged snapshots.
    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    ASSERT_EQ(removal.snaps.size(), unreferenced_count);
    ASSERT_TRUE(removal.refs.empty());
}

TEST_F(RemoveSnapshotsActionTest, TestRemoveExpiredTag) {
    const auto now = model::timestamp::now();
    auto table = create_table();
    auto id1 = add_snapshots(1, now.value(), &table);
    add_tag(id1, "tag1", &table);
    auto id2 = add_snapshots(1, now.value(), &table);
    add_tag(id2, "tag2", &table);

    // Create a bunch of tags with an reference expiry age.
    const auto expired_count = 42;
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    for (int i = 0; i < expired_count; ++i) {
        auto s = add_snapshots(1, now.value(), &table);
        auto& tag = add_tag(s, fmt::format("expired-{}", s), &table);
        tag.max_ref_age_ms = expiry_age_ms;
    }
    auto retained_removal = compute_removal(table, now.value() + expiry_age_ms);
    ASSERT_TRUE(retained_removal.snaps.empty());
    ASSERT_TRUE(retained_removal.refs.empty());

    // All the expired snapshots should be removed, while we keep the non
    // expired snapshots.
    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    ASSERT_EQ(removal.snaps.size(), expired_count);
    ASSERT_EQ(removal.refs.size(), expired_count);
}

TEST_F(RemoveSnapshotsActionTest, TestRemoveExpiredBranch) {
    const auto now = model::timestamp::now();
    auto table = create_table();
    auto id1 = add_snapshots(1, now.value(), &table);
    add_branch(id1, "branch1", &table);
    auto id2 = add_snapshots(1, now.value(), &table);
    add_branch(id2, "branch2", &table);

    // Create a bunch of branch refs with a reference expiry age.
    const auto snaps_per_expired_branch = 10;
    const auto expired_branches = 42;
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    for (int i = 0; i < expired_branches; ++i) {
        auto s = add_snapshots(snaps_per_expired_branch, now.value(), &table);
        auto& branch_ref = add_branch(s, fmt::format("expired-{}", s), &table);
        branch_ref.max_ref_age_ms = expiry_age_ms;
    }
    auto retained_removal = compute_removal(table, now.value() + expiry_age_ms);
    ASSERT_TRUE(retained_removal.snaps.empty());
    ASSERT_TRUE(retained_removal.refs.empty());

    // All the expired snapshots should be removed, while we keep the non
    // expired snapshots.
    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    ASSERT_EQ(
      removal.snaps.size(), expired_branches * snaps_per_expired_branch);
    ASSERT_EQ(removal.refs.size(), expired_branches);
}

TEST_F(RemoveSnapshotsActionTest, TestRetainTaggedBranch) {
    const auto now = model::timestamp::now();
    auto table = create_table();
    auto id = add_snapshots(1, now.value(), &table);

    // Create a tag, which defaults to being kept around forever.
    add_tag(id, "tag", &table);

    // Set the branch reference to expire quickly.
    auto& branch_ref = add_branch(id, "branch", &table);
    const auto expiry_age_ms = 1000;
    branch_ref.max_snapshot_age_ms = expiry_age_ms;
    branch_ref.max_ref_age_ms = expiry_age_ms;

    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);

    // We should remove the branch reference, and since that's removed, its
    // expiry of the snapshot is also voided.
    ASSERT_EQ(removal.snaps.size(), 0);
    ASSERT_EQ(removal.refs.size(), 1);
}

TEST_F(RemoveSnapshotsActionTest, TestRandomizedSnapshots) {
    auto table = create_table();
    // Set up two expiration points, one in the near future and one farther
    // out. This test will expire in two waves, at these two points.
    auto short_expiry_age_ms = 10000;
    auto long_expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;

    size_t num_expected_removed_short = 0;
    size_t num_expected_removed_long = 0;

    // Create some snapshots that aren't referenced. These are kept around
    // for the default expiry age.
    const auto now = model::timestamp::now();
    for (int i = 0; i < random_generators::get_int(1, 100); ++i) {
        add_snapshots(1, now.value(), &table);
        ++num_expected_removed_long;
    }

    // Create some snapshots that are referenced via tag. These are kept around
    // forever.
    for (int i = 0; i < random_generators::get_int(1, 100); ++i) {
        auto id = add_snapshots(1, now.value(), &table);
        add_tag(id, fmt::format("tag-{}", id), &table);
    }

    // Create some snapshots that are a part of a branch that expire in a short
    // amount of time.  We will keep the minimum count to retain.
    auto num_short_expired = random_generators::get_int(1, 100);
    auto branch1 = add_snapshots(num_short_expired, now.value(), &table);
    auto& branch1_ref = add_branch(branch1, "branch1", &table);
    branch1_ref.max_snapshot_age_ms = short_expiry_age_ms;
    num_expected_removed_short
      += (num_short_expired - remove_snapshots_action::default_min_snapshots_retained);
    num_expected_removed_long
      += (num_short_expired - remove_snapshots_action::default_min_snapshots_retained);

    // Create some snapshots that expire just after the short expiry age. We
    // will keep all of them, even if there is more than the minimum count to
    // retain.
    auto num_short_retained = random_generators::get_int(1, 100);
    auto branch2 = add_snapshots(num_short_retained, now.value(), &table);
    auto& branch2_ref = add_branch(branch2, "branch2", &table);
    branch2_ref.max_snapshot_age_ms = short_expiry_age_ms + 1;
    num_expected_removed_long
      += (num_short_retained - remove_snapshots_action::default_min_snapshots_retained);

    auto short_removal = compute_removal(
      table, now.value() + short_expiry_age_ms + 1);
    ASSERT_EQ(short_removal.snaps.size(), num_expected_removed_short);

    auto long_removal = compute_removal(
      table, now.value() + long_expiry_age_ms + 1);
    ASSERT_EQ(long_removal.snaps.size(), num_expected_removed_long);
}

TEST_F(RemoveSnapshotsActionTest, TestRemoveSnapshotTransaction) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms = 10000;
    auto table = create_table();
    auto highest_id = add_snapshots(
      default_total_snaps, now.value() - expiry_age_ms - 1, &table);
    auto& main_ref = add_branch(highest_id, "main", &table);
    main_ref.max_snapshot_age_ms = expiry_age_ms;

    ASSERT_EQ(table.snapshots->size(), default_total_snaps);
    ASSERT_EQ(table.refs->size(), 1);

    transaction txn(std::move(table));
    auto tx_outcome = txn.remove_expired_snapshots(now).get();
    ASSERT_FALSE(tx_outcome.has_error());

    // Removed snapshots should be returned as individual updates.
    ASSERT_NO_FATAL_FAILURE(
      check_expected_updates(txn, default_total_snaps - 1, 0));
    ASSERT_EQ(
      txn.table().snapshots->size(),
      remove_snapshots_action::default_min_snapshots_retained);
    ASSERT_EQ(txn.table().refs->size(), 1);
}

TEST_F(RemoveSnapshotsActionTest, TestRemoveSnapshotReferenceTransaction) {
    const auto now = model::timestamp::now();
    const auto expiry_age_ms
      = remove_snapshots_action::default_max_snapshot_age_ms;
    auto table = create_table();
    auto id = add_snapshots(1, now.value() - expiry_age_ms - 1, &table);
    auto& tag_ref = add_tag(id, "tag", &table);
    tag_ref.max_snapshot_age_ms = expiry_age_ms;
    tag_ref.max_ref_age_ms = expiry_age_ms;

    ASSERT_EQ(table.snapshots->size(), 1);
    ASSERT_EQ(table.refs->size(), 1);

    transaction txn(std::move(table));
    auto tx_outcome = txn.remove_expired_snapshots(now).get();
    ASSERT_FALSE(tx_outcome.has_error());

    ASSERT_NO_FATAL_FAILURE(check_expected_updates(txn, 1, 1));
    ASSERT_EQ(txn.table().snapshots->size(), 0);
    ASSERT_EQ(txn.table().refs->size(), 0);
}

TEST_F(RemoveSnapshotsActionTest, TestTableMaxAgeProperty) {
    const auto expiry_age_ms = 1000;
    auto table = create_table();
    table.properties.emplace();
    table.properties->emplace(
      max_snapshot_age_ms_prop, fmt::to_string(expiry_age_ms));

    const auto now = model::timestamp::now();
    auto highest_id = add_snapshots(default_total_snaps, now.value(), &table);
    add_branch(highest_id, "main", &table);

    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    EXPECT_EQ(
      removal.snaps.size(),
      default_total_snaps
        - remove_snapshots_action::default_min_snapshots_retained);
    EXPECT_TRUE(removal.refs.empty());
}

TEST_F(RemoveSnapshotsActionTest, TestTableMaxRefAgeProperty) {
    const auto expiry_age_ms = 1000;
    auto table = create_table();
    table.properties.emplace();
    // Set the snapshots and references to expire at the same time so snapshots
    // with expired references are removed earlier than the default.
    table.properties->emplace(
      max_ref_age_ms_prop, fmt::to_string(expiry_age_ms));
    table.properties->emplace(
      max_snapshot_age_ms_prop, fmt::to_string(expiry_age_ms));

    const auto now = model::timestamp::now();
    auto id = add_snapshots(1, now.value(), &table);
    add_tag(id, "tag", &table);

    auto removal = compute_removal(table, now.value() + expiry_age_ms + 1);
    EXPECT_EQ(removal.snaps.size(), 1);
    EXPECT_EQ(removal.refs.size(), 1);
}

TEST_F(RemoveSnapshotsActionTest, TestTableMinSnapsKeptProperty) {
    const auto min_to_keep = 10;
    auto table = create_table();
    table.properties.emplace();
    table.properties->emplace(
      min_snapshots_to_keep_prop, fmt::to_string(min_to_keep));

    const auto now = model::timestamp::now();
    auto highest_id = add_snapshots(default_total_snaps, now.value(), &table);
    add_branch(highest_id, "branch", &table);

    auto removal = compute_removal(
      table,
      now.value() + remove_snapshots_action::default_max_snapshot_age_ms + 1);
    EXPECT_EQ(removal.snaps.size(), default_total_snaps - min_to_keep);
    EXPECT_EQ(removal.refs.size(), 0);
}
