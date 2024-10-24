// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
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
          .location = "foo/bar",
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

    partition_key make_int_pk(int32_t v) {
        auto pk_struct = std::make_unique<struct_value>();
        pk_struct->fields.emplace_back(int_value{v});
        return {std::move(pk_struct)};
    }

    chunked_vector<data_file> create_data_files(
      const ss::sstring& path_base,
      size_t num_files,
      size_t record_count,
      int32_t pk_value = 42) {
        chunked_vector<data_file> ret;
        ret.reserve(num_files);
        const auto records_per_file = record_count / num_files;
        const auto leftover_records = record_count % num_files;
        for (size_t i = 0; i < num_files; i++) {
            const auto path = fmt::format("{}-{}", path_base, i);
            ret.emplace_back(data_file{
              .content_type = data_file_content_type::data,
              .file_path = path,
              .partition = partition_key{make_int_pk(pk_value)},
              .record_count = records_per_file,
              .file_size_bytes = 1_KiB,
            });
        }
        ret[0].record_count += leftover_records;
        return ret;
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    manifest_io io;
};

TEST_F(MergeAppendActionTest, TestMergeByCount) {
    const size_t num_to_merge_at
      = merge_append_action::default_min_to_merge_new_files;
    const size_t files_per_man = 2;
    const size_t rows_per_man = 25;
    transaction tx(io, create_table());
    // Repeatedly add new data files. Each iteration creates a new manifest
    // because we're below the merge count threshhold.
    for (size_t i = 0; i < num_to_merge_at; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        auto res = tx.merge_append(
                       create_data_files("foo", files_per_man, rows_per_man))
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
        const auto& table = tx.table();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);

        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist
          = io.download_manifest_list_uri(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist.has_value());
        ASSERT_EQ(latest_mlist.value().files.size(), expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest.
    auto res = tx.merge_append(
                   create_data_files("foo", files_per_man, rows_per_man))
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a single manifest.
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list_uri(latest_mlist_path).get();
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
    transaction tx(io, create_table());
    for (size_t i = 0; i < num_to_merge_at; i++) {
        const auto expected_snapshots = i + 1;
        const auto expected_manifests = expected_snapshots;
        auto res = tx.merge_append(create_data_files(
                                     path_base, files_per_man, rows_per_man))
                     .get();
        ASSERT_FALSE(res.has_error()) << res.error();
        const auto& table = tx.table();
        ASSERT_TRUE(table.snapshots.has_value());
        ASSERT_TRUE(table.current_snapshot_id.has_value());
        ASSERT_EQ(table.snapshots.value().size(), expected_snapshots);

        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist
          = io.download_manifest_list_uri(latest_mlist_path).get();
        ASSERT_TRUE(latest_mlist.has_value());
        ASSERT_EQ(latest_mlist.value().files.size(), expected_manifests);
    }

    // At the merge threshold, we expect the latest snapshot contains a merged
    // manifest.
    auto res = tx.merge_append(
                   create_data_files(path_base, files_per_man, rows_per_man))
                 .get();
    ASSERT_FALSE(res.has_error()) << res.error();
    const auto& table = tx.table();
    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots.value().size(), num_to_merge_at + 1);

    // Validate that the latest snapshot indeed contains a three manifests:
    // - the one containing 8 merged 1MB paths
    // - the two we added that weren't merged
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist = io.download_manifest_list_uri(latest_mlist_path).get();
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

TEST_F(MergeAppendActionTest, TestUniqueSnapshotIds) {
    transaction tx(io, create_table());
    const auto& table = tx.table();
    chunked_hash_set<snapshot_id> snap_ids;
    const size_t num_snapshots = 1000;
    for (auto i = 0; i < 1000; i++) {
        const auto expected_snapshots = i + 1;
        auto res = tx.merge_append(create_data_files("foo", 1, 1)).get();
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
    transaction tx(io, create_table());
    const auto& table = tx.table();
    chunked_hash_set<snapshot_id> snap_ids;
    const int32_t base_pk = 300;
    for (size_t i = 0; i < num_to_merge_at; i++) {
        int32_t pk = base_pk + i;
        const auto expected_manifests = i + 1;
        auto res = tx.merge_append(create_data_files("foo", 1, 1, pk)).get();
        ASSERT_FALSE(res.has_error()) << res.error();

        // Download the resulting manifest list and make sure we only added a
        // manifest (no merge yet).
        auto latest_mlist_path
          = table.snapshots.value().back().manifest_list_path;
        auto latest_mlist_res
          = io.download_manifest_list_uri(latest_mlist_path).get();
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
    auto res = tx.merge_append(create_data_files("foo", 1, 1, last_pk)).get();
    ASSERT_FALSE(res.has_error()) << res.error();
    auto latest_mlist_path = table.snapshots.value().back().manifest_list_path;
    auto latest_mlist_res
      = io.download_manifest_list_uri(latest_mlist_path).get();
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
        transaction tx(io, std::move(t));
        auto res = tx.merge_append(create_data_files("foo", 1, 1)).get();
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
        // More than one partition spec.
        auto t = create_table();
        t.partition_specs.emplace_back(partition_spec{});
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
    transaction tx(io, create_table());
    auto bad_files = create_data_files("foo", 1, 1);
    for (auto& f : bad_files) {
        f.partition = partition_key{};
    }
    auto res = tx.merge_append(std::move(bad_files)).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), action::errc::unexpected_state);
}
