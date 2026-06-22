/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "iceberg/compatibility.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/merge_append_action.h"
#include "iceberg/partition.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/transaction.h"
#include "iceberg/values_bytes.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

using namespace iceberg;
using namespace std::chrono_literals;

class MergeAppendActionTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    MergeAppendActionTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , io(remote(), bucket_name) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    table_metadata create_table() {
        auto s = schema{
          .schema_struct = std::get<struct_type>(test_nested_schema_type()),
          .schema_id = schema::id_t{0},
          .identifier_field_ids = {},
        };
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());
        chunked_vector<partition_spec> pspecs;
        pspecs.emplace_back(partition_spec{
          .spec_id = partition_spec::id_t{0},
          .fields = {
            // Creates a partition key of type [int].
            partition_field{
              .source_id = nested_field::id_t{2},
              .field_id = partition_field::id_t{1000},
              .name = "bar",
              .transform = identity_transform{},
            },
          },
        });
        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = uri(fmt::format("s3://{}/foo/bar", bucket_name())),
          .last_sequence_number = sequence_number{0},
          .last_updated_ms = model::timestamp::now(),
          .last_column_id = s.highest_field_id().value(),
          .schemas = std::move(schemas),
          .current_schema_id = schema::id_t{0},
          .partition_specs = std::move(pspecs),
          .default_spec_id = partition_spec::id_t{0},
          .last_partition_id = partition_field::id_t{-1},
        };
    }

    partition_key make_single_field_pk(primitive_value v) {
        auto pk_struct = std::make_unique<struct_value>();
        pk_struct->fields.emplace_back(std::move(v));
        return {std::move(pk_struct)};
    }

    chunked_vector<file_to_append> create_data_files(
      const table_metadata& md,
      const ss::sstring& path_base,
      size_t num_files,
      size_t record_count,
      primitive_value pk_value = int_value{42},
      std::optional<schema::id_t> schema_id = std::nullopt,
      std::optional<partition_spec::id_t> partition_spec_id = std::nullopt) {
        chunked_vector<file_to_append> ret;
        ret.reserve(num_files);
        const auto records_per_file = record_count / num_files;
        const auto leftover_records = record_count % num_files;
        for (size_t i = 0; i < num_files; i++) {
            const auto path = fmt::format("{}-{}", path_base, i);
            data_file file{
              .content_type = data_file_content_type::data,
              .file_path = uri(path),
              .partition = make_single_field_pk(make_copy(pk_value)),
              .record_count = records_per_file,
              .file_size_bytes = 1_KiB,
            };
            ret.emplace_back(
              file_to_append{
                .file = std::move(file),
                .schema_id = (schema_id ? *schema_id : md.current_schema_id),
                .partition_spec_id
                = (partition_spec_id ? *partition_spec_id : md.default_spec_id),
              });
        }
        ret[0].file.record_count += leftover_records;
        return ret;
    }

    void merge_append_and_check(
      transaction& tx,
      chunked_vector<file_to_append> files,
      size_t expected_snapshots,
      size_t expected_manifests) {
        auto res = tx.merge_append(io, std::move(files)).get();
        ASSERT_FALSE(res.has_error()) << res.error();
        const auto& table = tx.table();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);

        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist.has_value());
        ASSERT_EQ(latest_mlist.value().files.size(), expected_manifests);
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    manifest_io io;
};

TEST_F(MergeAppendActionTest, TestMergeByCount) {
    const size_t num_to_merge_at
      = merge_append_action::default_min_to_merge_new_files;
    const size_t files_per_man = 2;
    const size_t rows_per_man = 25;
    transaction tx(create_table());
    // Repeatedly add new data files. Each iteration creates a new manifest
    // because we're below the merge count threshhold.
    for (size_t i = 0; i < num_to_merge_at; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        auto res = tx.merge_append(
                       io,
                       create_data_files(
                         tx.table(), "foo", files_per_man, rows_per_man))
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
        const auto& table = tx.table();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);

        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist.has_value());
        ASSERT_EQ(latest_mlist.value().files.size(), expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest.
    auto res = tx.merge_append(
                   io,
                   create_data_files(
                     tx.table(), "foo", files_per_man, rows_per_man))
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a single manifest.
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
    ASSERT_TRUE(latest_mlist.has_value());
    ASSERT_EQ(latest_mlist.value().files.size(), 1);
    const auto& merged_mfile = latest_mlist.value().files[0];

    // Check that the manifest file's metadata seem sane.
    ASSERT_EQ(merged_mfile.partition_spec_id(), 0);
    ASSERT_EQ(merged_mfile.added_files_count, files_per_man);
    ASSERT_EQ(merged_mfile.added_rows_count, rows_per_man);
    ASSERT_EQ(
      merged_mfile.existing_files_count, num_to_merge_at * files_per_man);
    ASSERT_EQ(merged_mfile.existing_rows_count, num_to_merge_at * rows_per_man);
    ASSERT_EQ(merged_mfile.deleted_files_count, 0);
    ASSERT_EQ(merged_mfile.deleted_rows_count, 0);
}

TEST_F(MergeAppendActionTest, TestMergeByBytes) {
    // Inflate the datafile names so our manifest entries will be large, and
    // our manifest files will therefore be large.
    const ss::sstring path_base(1000000, 'x');
    // The default 8MiB merge threshold will allow for 8+1 1MB entries before
    // merging: the +1 is expected because when we add the 9th datafile, it
    // doesn't yet have a manifest, and is ignored for manifest bin-packing.
    const size_t num_to_merge_at = 9;
    const size_t files_per_man = 1;
    const size_t rows_per_man = 25;
    transaction tx(create_table());
    for (size_t i = 0; i < num_to_merge_at; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        auto res = tx.merge_append(
                       io,
                       create_data_files(
                         tx.table(), path_base, files_per_man, rows_per_man))
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
        const auto& table = tx.table();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);

        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist.has_value());
        ASSERT_EQ(latest_mlist.value().files.size(), expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest.
    auto res = tx.merge_append(
                   io,
                   create_data_files(
                     tx.table(), path_base, files_per_man, rows_per_man))
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a three manifests:
    // - the one containing 8 merged 1MB paths
    // - the two we added that weren't merged
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
    ASSERT_TRUE(latest_mlist.has_value());
    ASSERT_EQ(latest_mlist.value().files.size(), 3);

    // The newest file has the highest sequence number and add-counters ticked.
    const auto& latest_mfile = latest_mlist.value().files[0];
    ASSERT_EQ(latest_mfile.partition_spec_id(), 0);
    auto latest_seq = latest_mfile.seq_number;
    ASSERT_EQ(latest_seq(), 10);
    ASSERT_EQ(latest_mfile.min_seq_number, latest_mfile.seq_number);
    ASSERT_EQ(latest_mfile.added_files_count, files_per_man);
    ASSERT_EQ(latest_mfile.added_rows_count, rows_per_man);
    ASSERT_EQ(latest_mfile.existing_files_count, 0);
    ASSERT_EQ(latest_mfile.existing_rows_count, 0);
    ASSERT_EQ(latest_mfile.deleted_files_count, 0);
    ASSERT_EQ(latest_mfile.deleted_rows_count, 0);

    // The next file also has add-counters ticked because it wasn't updated
    // after its initial write, also indicated by the lower sequence number.
    const auto& middle_mfile = latest_mlist.value().files[1];
    ASSERT_EQ(middle_mfile.seq_number(), latest_seq() - 1);
    ASSERT_EQ(middle_mfile.min_seq_number, middle_mfile.seq_number);
    ASSERT_EQ(middle_mfile.partition_spec_id(), 0);
    ASSERT_EQ(middle_mfile.added_files_count, files_per_man);
    ASSERT_EQ(middle_mfile.added_rows_count, rows_per_man);
    ASSERT_EQ(middle_mfile.existing_files_count, 0);
    ASSERT_EQ(middle_mfile.existing_rows_count, 0);
    ASSERT_EQ(middle_mfile.deleted_files_count, 0);
    ASSERT_EQ(middle_mfile.deleted_rows_count, 0);

    // The last file is the merged file.
    const auto& merged_mfile = latest_mlist.value().files[2];
    ASSERT_EQ(merged_mfile.seq_number(), latest_seq());
    ASSERT_EQ(merged_mfile.min_seq_number(), 1);
    ASSERT_EQ(merged_mfile.partition_spec_id(), 0);
    ASSERT_EQ(merged_mfile.added_files_count, 0);
    ASSERT_EQ(merged_mfile.added_rows_count, 0);
    ASSERT_EQ(merged_mfile.existing_files_count, 8 * files_per_man);
    ASSERT_EQ(merged_mfile.existing_rows_count, 8 * rows_per_man);
    ASSERT_EQ(merged_mfile.deleted_files_count, 0);
    ASSERT_EQ(merged_mfile.deleted_rows_count, 0);
}

TEST_F(MergeAppendActionTest, TestMergeAfterTypePromotion) {
    const size_t num_to_merge_at
      = merge_append_action::default_min_to_merge_new_files;
    const size_t files_per_man = 2;
    const size_t rows_per_man = 25;
    transaction tx(create_table());
    // Repeatedly add new data files up to the merge count threshold. First half
    // of the files will be with the old schema.
    for (size_t i = 0; i < num_to_merge_at / 2; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        merge_append_and_check(
          tx,
          create_data_files(tx.table(), "foo", files_per_man, rows_per_man),
          expected_snapshots,
          expected_manifests);
    }

    // Promote int partition column to long.
    {
        auto orig_type = tx.table().schemas.at(0).schema_struct.copy();
        auto new_type = orig_type.copy();
        bool found = false;
        for (auto& field : new_type.fields) {
            if (field->name == "bar") {
                field->type = long_type{};
                found = true;
            }
        }
        ASSERT_TRUE(found);

        auto compat_res = evolve_schema(
          orig_type,
          new_type,
          tx.table().partition_specs.back(),
          field_name_comparison::verbatim);
        ASSERT_FALSE(compat_res.has_error());

        auto res = tx.set_schema(
                       iceberg::schema{
                         .schema_struct = std::move(new_type),
                         .identifier_field_ids = {},
                       })
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
    }

    // Add the remaining files, now partition column value is long.
    for (size_t i = num_to_merge_at / 2; i < num_to_merge_at; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        merge_append_and_check(
          tx,
          create_data_files(
            tx.table(), "foo", files_per_man, rows_per_man, long_value{42}),
          expected_snapshots,
          expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest. All manifests should be successfully merged, even though the
    // type of the partition column is not the same.
    auto res = tx.merge_append(
                   io,
                   create_data_files(
                     tx.table(), "foo", files_per_man, rows_per_man))
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a single manifest.
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
    ASSERT_TRUE(latest_mlist.has_value());
    ASSERT_EQ(latest_mlist.value().files.size(), 1);
    const auto& merged_mfile = latest_mlist.value().files[0];

    // Check that the manifest file's metadata seem sane.
    ASSERT_EQ(merged_mfile.partition_spec_id(), 0);

    ASSERT_EQ(merged_mfile.partitions.size(), 1);
    ASSERT_FALSE(merged_mfile.partitions[0].contains_nan);
    ASSERT_FALSE(merged_mfile.partitions[0].contains_null);
    auto partition_bytes = value_to_bytes(long_value{42});
    ASSERT_EQ(merged_mfile.partitions[0].lower_bound, partition_bytes);
    ASSERT_EQ(merged_mfile.partitions[0].upper_bound, partition_bytes);

    ASSERT_EQ(merged_mfile.added_files_count, files_per_man);
    ASSERT_EQ(merged_mfile.added_rows_count, rows_per_man);
    ASSERT_EQ(
      merged_mfile.existing_files_count, num_to_merge_at * files_per_man);
    ASSERT_EQ(merged_mfile.existing_rows_count, num_to_merge_at * rows_per_man);
    ASSERT_EQ(merged_mfile.deleted_files_count, 0);
    ASSERT_EQ(merged_mfile.deleted_rows_count, 0);
}

TEST_F(MergeAppendActionTest, TestUniqueSnapshotIds) {
    transaction tx(create_table());
    const auto& table = tx.table();
    chunked_hash_set<snapshot_id> snap_ids;
    const size_t num_snapshots = 1000;
    for (auto i = 0; i < 1000; i++) {
        const auto expected_snapshots = i + 1;
        auto res = tx.merge_append(
                       io, create_data_files(tx.table(), "foo", 1, 1))
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);
        ASSERT_EQ(table.snapshots->back().id, *table.current_snapshot_id);

        // Each snapshot should get a unique snapshot id.
        ASSERT_TRUE(snap_ids.emplace(table.current_snapshot_id.value()).second);
    }
    // Sanity check that the snapshots' ids match with what we collected when
    // building the snapshots.
    ASSERT_EQ(num_snapshots, snap_ids.size());
    const auto& snaps = table.snapshots.value();
    chunked_hash_set<snapshot_id> snap_ids_from_snaps;
    for (const auto& snap : snaps) {
        ASSERT_TRUE(snap_ids_from_snaps.emplace(snap.id).second);
    }
    ASSERT_EQ(snap_ids, snap_ids_from_snaps);
}

TEST_F(MergeAppendActionTest, TestPartitionSummaries) {
    const size_t num_to_merge_at
      = merge_append_action::default_min_to_merge_new_files;
    transaction tx(create_table());
    const auto& table = tx.table();
    chunked_hash_set<snapshot_id> snap_ids;
    const int32_t base_pk = 300;
    for (size_t i = 0; i < num_to_merge_at; i++) {
        int32_t pk = base_pk + i;
        const auto expected_manifests = i + 1;
        auto res
          = tx.merge_append(
                io, create_data_files(tx.table(), "foo", 1, 1, int_value{pk}))
              .get();
        ASSERT_FALSE(res.has_error()) << res.error();

        // Download the resulting manifest list and make sure we only added a
        // manifest (no merge yet).
        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist_res
          = io.download_manifest_list(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist_res.has_value());
        ASSERT_EQ(expected_manifests, latest_mlist_res.value().files.size());

        // Do some validation on the partition summaries.
        const auto& latest_mfile = latest_mlist_res.value().files.front();
        ASSERT_EQ(0, latest_mfile.partition_spec_id());
        const auto& latest_partitions = latest_mfile.partitions;
        ASSERT_EQ(1, latest_partitions.size());
        ASSERT_FALSE(latest_partitions[0].contains_nan);
        ASSERT_FALSE(latest_partitions[0].contains_null);

        // Since we haven't merged any files, we expect the latest manifest to
        // only contain data from the files we just appended, which span only a
        // single partition.
        auto lower_bound = latest_partitions[0].lower_bound;
        auto upper_bound = latest_partitions[0].upper_bound;
        ASSERT_TRUE(lower_bound.has_value());
        ASSERT_EQ(lower_bound, upper_bound);
        ASSERT_EQ(lower_bound.value(), value_to_bytes(int_value{pk}));
    }
    // Write one more manifest, triggering a merge.
    const int32_t last_pk = base_pk + num_to_merge_at + 123;
    auto res
      = tx.merge_append(
            io, create_data_files(tx.table(), "foo", 1, 1, int_value{last_pk}))
          .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist_res = io.download_manifest_list(latest_mlist_path).get();
    ASSERT_TRUE(latest_mlist_res.has_value());
    ASSERT_EQ(1, latest_mlist_res.value().files.size());

    // The resulting merged manifest should include partition information from
    // the prior manifest files.
    const auto& merged_mfile = latest_mlist_res.value().files[0];
    ASSERT_EQ(0, merged_mfile.partition_spec_id());
    const auto& latest_partitions = merged_mfile.partitions;
    ASSERT_EQ(1, latest_partitions.size());
    ASSERT_FALSE(latest_partitions[0].contains_nan);
    ASSERT_FALSE(latest_partitions[0].contains_null);
    ASSERT_EQ(
      latest_partitions[0].lower_bound, value_to_bytes(int_value{base_pk}));
    ASSERT_EQ(
      latest_partitions[0].upper_bound, value_to_bytes(int_value{last_pk}));
}

TEST_F(MergeAppendActionTest, TestBadMetadata) {
    chunked_vector<table_metadata> bad_tables;
    auto check_bad = [this](table_metadata t) {
        transaction tx(std::move(t));
        auto res = tx.merge_append(
                       io, create_data_files(tx.table(), "foo", 1, 1))
                     .get();
        ASSERT_TRUE(res.has_error());
        ASSERT_EQ(res.error(), action::errc::unexpected_state);
    };
    {
        // No schemas.
        auto t = create_table();
        t.schemas.clear();
        ASSERT_NO_FATAL_FAILURE(check_bad(std::move(t)));
    }
    {
        // Current snapshot is bogus.
        auto t = create_table();
        t.current_snapshot_id = snapshot_id{1234};
        ASSERT_NO_FATAL_FAILURE(check_bad(std::move(t)));
    }
    {
        // No partition specs.
        auto t = create_table();
        t.partition_specs.clear();
        ASSERT_NO_FATAL_FAILURE(check_bad(std::move(t)));
    }
    {
        // Current schema is bogus.
        auto t = create_table();
        t.current_schema_id = schema::id_t{1234};
        ASSERT_NO_FATAL_FAILURE(check_bad(std::move(t)));
    }
    {
        // Current spec is bogus.
        auto t = create_table();
        t.default_spec_id = partition_spec::id_t{1234};
        ASSERT_NO_FATAL_FAILURE(check_bad(std::move(t)));
    }
}

TEST_F(MergeAppendActionTest, TestBadFile) {
    transaction tx(create_table());
    auto bad_files = create_data_files(tx.table(), "foo", 1, 1);
    for (auto& f : bad_files) {
        f.file.partition = partition_key{};
    }
    auto res = tx.merge_append(io, std::move(bad_files)).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), action::errc::unexpected_state);
}

TEST_F(MergeAppendActionTest, TestTagSnapshot) {
    transaction tx(create_table());
    const auto& table = tx.table();
    auto res = tx.merge_append(
                   io,
                   create_data_files(tx.table(), "foo", 1, 1),
                   /*snapshot_props=*/{},
                   "tag")
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_TRUE(table.current_snapshot_id.has_value());

    auto snap_id = table.current_snapshot_id.value();
    ASSERT_TRUE(table.refs.has_value());
    ASSERT_TRUE(table.refs->contains("main"));
    ASSERT_TRUE(table.refs->contains("tag"));

    // Sanity check, main is always updated.
    auto main_snap = table.refs->at("main");
    ASSERT_EQ(snap_id, main_snap.snapshot_id);
    ASSERT_EQ(main_snap.type, snapshot_ref_type::branch);

    // Since we passed a tag, it should exist.
    auto tag_snap = table.refs->at("tag");
    ASSERT_EQ(snap_id, tag_snap.snapshot_id);
    ASSERT_EQ(tag_snap.type, snapshot_ref_type::tag);

    // Merge again with a tag.
    res = tx.merge_append(
              io,
              create_data_files(tx.table(), "foo", 1, 1),
              /*snapshot_props=*/{},
              "tag")
            .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_TRUE(table.current_snapshot_id.has_value());
    snap_id = table.current_snapshot_id.value();

    // The snapshot references should follow.
    tag_snap = table.refs->at("tag");
    ASSERT_EQ(snap_id, tag_snap.snapshot_id);
    ASSERT_EQ(tag_snap.type, snapshot_ref_type::tag);

    // Now merge with a different tag. The old tag shouldn't be affected.
    res = tx.merge_append(
              io,
              create_data_files(tx.table(), "foo", 1, 1),
              /*snapshot_props=*/{},
              /*tag_name=*/"other")
            .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_TRUE(table.current_snapshot_id.has_value());
    auto old_snap_id = snap_id;
    snap_id = table.current_snapshot_id.value();
    ASSERT_NE(old_snap_id, snap_id);

    // The new tag should have a new snapshot id.
    auto other_snap = table.refs->at("other");
    ASSERT_EQ(snap_id, other_snap.snapshot_id);
    ASSERT_EQ(other_snap.type, snapshot_ref_type::tag);

    // The old tag should refer to the last snapshot that was appended with it.
    tag_snap = table.refs->at("tag");
    ASSERT_EQ(old_snap_id, tag_snap.snapshot_id);
    ASSERT_EQ(tag_snap.type, snapshot_ref_type::tag);
}

TEST_F(MergeAppendActionTest, TestTagWithExpiration) {
    transaction tx(create_table());
    const auto& table = tx.table();
    chunked_hash_set<snapshot_id> snap_ids;
    // Add a snapshot without an explicit tag expiration.
    auto res = tx.merge_append(
                   io,
                   create_data_files(tx.table(), "foo", 1, 1),
                   /*snapshot_props=*/{},
                   "tag")
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_TRUE(table.current_snapshot_id.has_value());

    auto snap_id = table.current_snapshot_id.value();
    ASSERT_TRUE(table.refs.has_value());
    ASSERT_TRUE(table.refs->contains("tag"));

    // Sanity check, no snapshot reference properties are set.
    auto tag_snap = table.refs->at("tag");
    ASSERT_EQ(snap_id, tag_snap.snapshot_id);
    ASSERT_EQ(tag_snap.type, snapshot_ref_type::tag);
    ASSERT_FALSE(tag_snap.max_snapshot_age_ms.has_value());
    ASSERT_FALSE(tag_snap.min_snapshots_to_keep.has_value());
    ASSERT_FALSE(tag_snap.max_ref_age_ms.has_value());

    // Now try again with a tag expiration.
    auto long_max = std::numeric_limits<long>::max();
    res = tx.merge_append(
              io,
              create_data_files(tx.table(), "foo", 1, 1),
              /*snapshot_props=*/{},
              "tag",
              /*tag_expiration_ms=*/long_max)
            .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_TRUE(table.current_snapshot_id.has_value());

    snap_id = table.current_snapshot_id.value();
    ASSERT_TRUE(table.refs.has_value());
    ASSERT_TRUE(table.refs->contains("tag"));

    // Sanity check, just the reference expiration is set.
    tag_snap = table.refs->at("tag");
    ASSERT_EQ(snap_id, tag_snap.snapshot_id);
    ASSERT_EQ(tag_snap.type, snapshot_ref_type::tag);
    ASSERT_FALSE(tag_snap.max_snapshot_age_ms.has_value());
    ASSERT_FALSE(tag_snap.min_snapshots_to_keep.has_value());
    ASSERT_TRUE(tag_snap.max_ref_age_ms.has_value());
    ASSERT_EQ(long_max, tag_snap.max_ref_age_ms.value());
}

// Test appending with multiple partition specs, but not enough files to trigger
// merging.
TEST_F(MergeAppendActionTest, TestMultiplePartitionSpecs) {
    transaction tx(create_table());

    {
        // Append some files
        auto files = create_data_files(tx.table(), "foo", 2, 3);
        merge_append_and_check(tx, std::move(files), 1, 1);
    }

    {
        // Add second spec
        auto new_spec = unresolved_partition_spec{};
        new_spec.fields.push_back(
          unresolved_partition_spec::field{
            .source_name = {"baz"},
            .transform = identity_transform{},
            .name = "baz",
          });
        auto res = tx.set_partition_spec(std::move(new_spec)).get();
        ASSERT_FALSE(res.has_error()) << res.error();
        ASSERT_EQ(tx.table().partition_specs.size(), 2);
    }

    {
        // Append some more files with the new spec
        auto files = create_data_files(
          tx.table(), "foo", 2, 3, boolean_value{true});
        merge_append_and_check(tx, std::move(files), 2, 2);
    }

    {
        // Add third spec
        auto new_spec = unresolved_partition_spec{};
        new_spec.fields.push_back(
          unresolved_partition_spec::field{
            .source_name = {"foo"},
            .transform = identity_transform{},
            .name = "foo",
          });
        auto res = tx.set_partition_spec(std::move(new_spec)).get();
        ASSERT_FALSE(res.has_error()) << res.error();
        ASSERT_EQ(tx.table().partition_specs.size(), 3);
    }

    {
        // Append files with every partition spec in one action.
        chunked_vector<file_to_append> files;
        auto add_files = [&](
                           partition_spec::id_t spec_id,
                           primitive_value pk_value) {
            auto to_add = create_data_files(
              tx.table(),
              "foo",
              1,
              3,
              std::move(pk_value),
              std::nullopt,
              spec_id);
            std::move(to_add.begin(), to_add.end(), std::back_inserter(files));
        };
        add_files(partition_spec::id_t{0}, int_value{21});
        add_files(partition_spec::id_t{1}, boolean_value{false});
        add_files(partition_spec::id_t{2}, string_value{iobuf::from("aaa")});

        merge_append_and_check(tx, std::move(files), 3, 5);
    }
}

TEST_F(MergeAppendActionTest, TestMergeWithMultiplePartitionSpecs) {
    const size_t num_to_merge_at
      = merge_append_action::default_min_to_merge_new_files;
    const size_t files_per_man = 2;
    const size_t rows_per_man = 25;
    transaction tx(create_table());

    auto files_with_first_spec = [&]() {
        return create_data_files(
          tx.table(),
          "foo",
          files_per_man,
          rows_per_man,
          int_value{42},
          tx.table().schemas.at(0).schema_id,
          tx.table().partition_specs.at(0).spec_id);
    };

    // Repeatedly add 50 manifests with old partition spec.
    for (size_t i = 0; i < num_to_merge_at / 2; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        merge_append_and_check(
          tx, files_with_first_spec(), expected_snapshots, expected_manifests);
    }

    {
        // Add new partition spec and make it default.
        auto table = std::move(tx).release_metadata();
        auto new_spec = partition_spec{.spec_id = partition_spec::id_t{1}};
        new_spec.fields.push_back(
          partition_field{
            .source_id = nested_field::id_t{3}, // baz
            .field_id = partition_field::id_t{1001},
            .name = "baz",
            .transform = identity_transform{},
          });
        table.partition_specs.push_back(std::move(new_spec));
        table.default_spec_id = table.partition_specs.back().spec_id;
        tx = transaction(std::move(table));

        // delete the field used by the old spec
        const auto& orig_type = tx.table().schemas.at(0).schema_struct.copy();
        chunked_vector<nested_field_ptr> fields_copy;
        for (const auto& f : orig_type.fields) {
            if (f->name != "bar") {
                fields_copy.emplace_back(f->copy());
            }
        }
        struct_type new_type{.fields = std::move(fields_copy)};
        auto compat_res = evolve_schema(
          orig_type,
          new_type,
          tx.table().partition_specs.back(),
          field_name_comparison::verbatim);
        ASSERT_FALSE(compat_res.has_error());

        auto res = tx.set_schema(
                       iceberg::schema{
                         .schema_struct = std::move(new_type),
                         .identifier_field_ids = {},
                       })
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
    }

    auto files_with_second_spec = [&]() {
        return create_data_files(
          tx.table(),
          "foo",
          files_per_man,
          rows_per_man,
          boolean_value{true},
          tx.table().schemas.at(1).schema_id,
          tx.table().partition_specs.at(1).spec_id);
    };

    auto files_with_both_specs = [&]() {
        auto ret = files_with_first_spec();
        auto more_files = files_with_second_spec();
        std::move(
          more_files.begin(), more_files.end(), std::back_inserter(ret));
        return ret;
    };

    // Add more files with both specs. Number of manifests with the old spec
    // will reach the merge threshold.
    for (size_t i = 0; i < num_to_merge_at / 2; i++) {
        // each merge_append here adds 1 snapshot and 2 manifests
        const auto expected_snapshots = num_to_merge_at / 2 + i + 1;
        const auto expected_manifests = num_to_merge_at / 2 + 1 + 2 * i + 1;
        merge_append_and_check(
          tx, files_with_both_specs(), expected_snapshots, expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest for the old spec and the initial set of unmerged manifests for
    // the new spec.
    auto res = tx.merge_append(io, files_with_both_specs()).get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a single manifest for
    // the old spec.
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list(latest_mlist_path).get();
    ASSERT_TRUE(latest_mlist.has_value());
    ASSERT_EQ(latest_mlist.value().files.size(), num_to_merge_at / 2 + 2);

    const manifest_file* merged_mfile = nullptr;
    for (const auto& m : latest_mlist.value().files) {
        if (m.partition_spec_id() == 0) {
            ASSERT_FALSE(merged_mfile);
            merged_mfile = &m;
        } else {
            // still unmerged files with new partition spec
            ASSERT_EQ(m.partition_spec_id(), 1);
            ASSERT_EQ(m.added_files_count, files_per_man);
            ASSERT_EQ(m.added_rows_count, rows_per_man);
            ASSERT_EQ(m.existing_files_count, 0);
            ASSERT_EQ(m.existing_rows_count, 0);
            ASSERT_EQ(m.deleted_files_count, 0);
            ASSERT_EQ(m.deleted_rows_count, 0);
        }
    }
    ASSERT_TRUE(merged_mfile);

    // Check that the manifest file's metadata seem sane.
    ASSERT_EQ(merged_mfile->partition_spec_id(), 0);
    ASSERT_EQ(merged_mfile->added_files_count, files_per_man);
    ASSERT_EQ(merged_mfile->added_rows_count, rows_per_man);
    ASSERT_EQ(
      merged_mfile->existing_files_count, num_to_merge_at * files_per_man);
    ASSERT_EQ(
      merged_mfile->existing_rows_count, num_to_merge_at * rows_per_man);
    ASSERT_EQ(merged_mfile->deleted_files_count, 0);
    ASSERT_EQ(merged_mfile->deleted_rows_count, 0);
}

TEST_F(MergeAppendActionTest, TestWriteMetadataPathProperty) {
    // Create a table with a custom metadata path.
    const auto custom_path = fmt::format(
      "s3://{}/custom/metadata", bucket_name());
    auto table = create_table();
    table_properties_t props;
    props.emplace("write.metadata.path", custom_path);
    table.properties = std::move(props);

    // Add some data files to trigger manifest creation.
    transaction tx(std::move(table));
    auto files = create_data_files(tx.table(), "test_file", 2, 10);
    auto res = tx.merge_append(io, std::move(files)).get();
    ASSERT_FALSE(res.has_error()) << res.error();

    const auto& updated_table = tx.table();
    ASSERT_TRUE(updated_table.snapshots.has_value());
    ASSERT_EQ(updated_table.snapshots->size(), 1);

    // Check that the manifest list path uses the custom metadata location
    const auto& snapshot = updated_table.snapshots->back();
    const auto& manifest_list_path = snapshot.manifest_list_path;
    ASSERT_TRUE(manifest_list_path().starts_with(custom_path));

    // Download the manifest list and check manifest paths.
    auto mlist_res = io.download_manifest_list(manifest_list_path).get();
    ASSERT_TRUE(mlist_res.has_value());
    ASSERT_EQ(mlist_res.value().files.size(), 1);

    const auto& manifest_path = mlist_res.value().files[0].manifest_path;
    ASSERT_TRUE(manifest_path().starts_with(custom_path));
}

TEST_F(MergeAppendActionTest, TestSnapshotSummaryFirstSnapshot) {
    transaction tx(create_table());
    const size_t files_count = 3;
    const size_t records_count = 150;

    auto files = create_data_files(
      tx.table(), "test", files_count, records_count);
    auto res = tx.merge_append(io, std::move(files)).get();
    ASSERT_FALSE(res.has_error()) << res.error();

    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    const auto& summary = table.snapshots->back().summary;

    // First snapshot: totals should equal added values
    const size_t file_size = 1_KiB * files_count;
    ASSERT_EQ(summary.operation, snapshot_operation::append);
    ASSERT_EQ(summary.added_data_files, files_count);
    ASSERT_EQ(summary.added_records, records_count);
    ASSERT_EQ(summary.added_files_size, file_size);
    ASSERT_EQ(summary.total_data_files, files_count);
    ASSERT_EQ(summary.total_records, records_count);
    ASSERT_EQ(summary.total_files_size, file_size);
}

TEST_F(MergeAppendActionTest, TestSnapshotSummaryAdds) {
    transaction tx(create_table());

    // First append.
    const size_t first_files = 2;
    const size_t first_records = 100;
    auto files1 = create_data_files(
      tx.table(), "test1", first_files, first_records);
    auto res1 = tx.merge_append(io, std::move(files1)).get();
    ASSERT_FALSE(res1.has_error());

    // Second append.
    const size_t second_files = 3;
    const size_t second_records = 200;
    auto files2 = create_data_files(
      tx.table(), "test2", second_files, second_records);
    auto res2 = tx.merge_append(io, std::move(files2)).get();
    ASSERT_FALSE(res2.has_error());

    const auto& table = tx.table();
    const auto& latest_summary = table.snapshots->back().summary;

    // The second snapshot should add onto existing totals.
    ASSERT_EQ(latest_summary.added_data_files, second_files);
    ASSERT_EQ(latest_summary.added_records, second_records);
    ASSERT_EQ(latest_summary.total_data_files, first_files + second_files);
    ASSERT_EQ(latest_summary.total_records, first_records + second_records);
    ASSERT_EQ(
      latest_summary.total_files_size, 1_KiB * (first_files + second_files));
}

TEST_F(MergeAppendActionTest, TestSnapshotSummaryMissingMetrics) {
    auto table = create_table();
    auto initial_mlist_path = uri(
      fmt::format("s3://{}/manifest1.avro", bucket_name()));
    snapshot initial_snap{
        .id = snapshot_id{1},
        .sequence_number = sequence_number{1},
        .timestamp_ms = model::timestamp::now(),
        .summary = snapshot_summary{
            .operation = snapshot_operation::append,
            // Only set total_records, leave others unset.
            .total_records = 100,
        },
        .manifest_list_path = initial_mlist_path,
        .schema_id = table.current_schema_id,
    };
    manifest_list initial_mlist;
    auto up_res
      = io.upload_manifest_list(initial_mlist_path, initial_mlist).get();
    ASSERT_TRUE(up_res.has_value());

    table.snapshots = chunked_vector<snapshot>{};
    table.snapshots->emplace_back(std::move(initial_snap));
    table.current_snapshot_id = snapshot_id{1};
    table.last_sequence_number = sequence_number{1};

    transaction tx(std::move(table));
    auto files = create_data_files(tx.table(), "test", 1, 50);
    auto res = tx.merge_append(io, std::move(files)).get();
    ASSERT_FALSE(res.has_error()) << res.error();

    const auto& updated_table = tx.table();
    const auto& latest_summary = updated_table.snapshots->back().summary;

    // We should only update the total_records metric, since the others were
    // missing.
    ASSERT_EQ(latest_summary.total_records, 150);
    ASSERT_FALSE(latest_summary.total_data_files.has_value());
    ASSERT_FALSE(latest_summary.total_files_size.has_value());
}
