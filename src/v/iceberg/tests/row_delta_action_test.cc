/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/merge_append_action.h"
#include "iceberg/partition.h"
#include "iceberg/row_delta_action.h"
#include "iceberg/table_update.h"
#include "iceberg/table_update_applier.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/values_bytes.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

using namespace iceberg;

class RowDeltaActionTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    RowDeltaActionTest()
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
      primitive_value pk_value = int_value{42}) {
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
                .schema_id = md.current_schema_id,
                .partition_spec_id = md.default_spec_id,
              });
        }
        ret[0].file.record_count += leftover_records;
        return ret;
    }

    chunked_vector<file_to_delete> create_delete_files(
      const table_metadata& md,
      const ss::sstring& path_base,
      size_t num_files,
      size_t record_count,
      data_file_content_type content_type
      = data_file_content_type::position_deletes,
      primitive_value pk_value = int_value{42}) {
        chunked_vector<file_to_delete> ret;
        ret.reserve(num_files);
        const auto records_per_file = record_count / num_files;
        const auto leftover_records = record_count % num_files;
        for (size_t i = 0; i < num_files; i++) {
            const auto path = fmt::format("{}-del-{}", path_base, i);
            data_file file{
              .content_type = content_type,
              .file_path = uri(path),
              .partition = make_single_field_pk(make_copy(pk_value)),
              .record_count = records_per_file,
              .file_size_bytes = 512,
            };
            ret.emplace_back(
              file_to_delete{
                .file = std::move(file),
                .schema_id = md.current_schema_id,
                .partition_spec_id = md.default_spec_id,
              });
        }
        ret[0].file.record_count += leftover_records;
        return ret;
    }

    /// Runs a row_delta_action against the given table metadata, applying
    /// the resulting updates in place.
    checked<std::monostate, action::errc> run_row_delta(
      table_metadata& table,
      chunked_vector<file_to_append> data_files,
      chunked_vector<file_to_delete> delete_files) {
        row_delta_action rda(
          io, table, std::move(data_files), std::move(delete_files));
        action& a = rda;
        auto outcome = std::move(a).build_updates().get();
        if (outcome.has_error()) {
            return outcome.error();
        }
        auto& updates = outcome.value();
        for (auto& u : updates.updates) {
            auto res = table_update::apply(u, table);
            if (res != table_update::outcome::success) {
                return action::errc::unexpected_state;
            }
        }
        return std::monostate{};
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    manifest_io io;
};

TEST_F(RowDeltaActionTest, TestDeleteOnly) {
    auto table = create_table();
    const size_t delete_files_count = 3;
    const size_t deleted_records = 90;

    auto del_files = create_delete_files(
      table, "test", delete_files_count, deleted_records);
    auto res = run_row_delta(table, {}, std::move(del_files));
    ASSERT_FALSE(res.has_error()) << res.error();

    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots->size(), 1);
    const auto& snap = table.snapshots->back();
    ASSERT_EQ(snap.summary.operation, snapshot_operation::delete_data);

    // Verify summary fields.
    ASSERT_EQ(snap.summary.added_data_files, 0);
    ASSERT_EQ(snap.summary.added_delete_files, delete_files_count);
    ASSERT_EQ(snap.summary.deleted_records, deleted_records);
    ASSERT_EQ(snap.summary.total_delete_files, delete_files_count);
    ASSERT_EQ(snap.summary.total_data_files, 0);
    ASSERT_EQ(snap.summary.total_records, 0);

    // Verify manifest list has one delete manifest.
    auto mlist_res = io.download_manifest_list(snap.manifest_list_path).get();
    ASSERT_TRUE(mlist_res.has_value());
    const auto& mlist = mlist_res.value();
    ASSERT_EQ(mlist.files.size(), 1);
    ASSERT_EQ(mlist.files[0].content, manifest_file_content::deletes);
    ASSERT_EQ(mlist.files[0].added_files_count, delete_files_count);
    ASSERT_EQ(mlist.files[0].added_rows_count, deleted_records);

    // Download the manifest and verify entries.
    auto manifest_res
      = io.download_manifest(mlist.files[0].manifest_path).get();
    ASSERT_TRUE(manifest_res.has_value());
    const auto& manifest = manifest_res.value();
    ASSERT_EQ(manifest.entries.size(), delete_files_count);
    for (const auto& entry : manifest.entries) {
        ASSERT_EQ(entry.status, manifest_entry_status::added);
        ASSERT_EQ(
          entry.data_file.content_type,
          data_file_content_type::position_deletes);
    }
}

TEST_F(RowDeltaActionTest, TestDataAndDeletes) {
    auto table = create_table();
    const size_t data_files_count = 2;
    const size_t data_records = 100;
    const size_t delete_files_count = 1;
    const size_t deleted_records = 30;

    auto data = create_data_files(
      table, "data", data_files_count, data_records);
    auto deletes = create_delete_files(
      table, "del", delete_files_count, deleted_records);

    auto res = run_row_delta(table, std::move(data), std::move(deletes));
    ASSERT_FALSE(res.has_error()) << res.error();

    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots->size(), 1);
    const auto& snap = table.snapshots->back();
    ASSERT_EQ(snap.summary.operation, snapshot_operation::overwrite);

    // Verify summary.
    ASSERT_EQ(snap.summary.added_data_files, data_files_count);
    ASSERT_EQ(snap.summary.added_records, data_records);
    ASSERT_EQ(snap.summary.added_delete_files, delete_files_count);
    ASSERT_EQ(snap.summary.deleted_records, deleted_records);
    ASSERT_EQ(snap.summary.total_data_files, data_files_count);
    ASSERT_EQ(snap.summary.total_records, data_records);
    ASSERT_EQ(snap.summary.total_delete_files, delete_files_count);

    // Verify manifest list has both data and delete manifest files.
    auto mlist_res = io.download_manifest_list(snap.manifest_list_path).get();
    ASSERT_TRUE(mlist_res.has_value());
    const auto& mlist = mlist_res.value();
    ASSERT_EQ(mlist.files.size(), 2);

    size_t data_manifest_count = 0;
    size_t delete_manifest_count = 0;
    for (const auto& mf : mlist.files) {
        if (mf.content == manifest_file_content::data) {
            ++data_manifest_count;
            ASSERT_EQ(mf.added_files_count, data_files_count);
            ASSERT_EQ(mf.added_rows_count, data_records);
        } else {
            ++delete_manifest_count;
            ASSERT_EQ(mf.added_files_count, delete_files_count);
            ASSERT_EQ(mf.added_rows_count, deleted_records);
        }
    }
    ASSERT_EQ(data_manifest_count, 1);
    ASSERT_EQ(delete_manifest_count, 1);
}

TEST_F(RowDeltaActionTest, TestDataOnlyBehavesLikeAppend) {
    auto table = create_table();
    const size_t data_files_count = 3;
    const size_t data_records = 150;

    auto data = create_data_files(
      table, "data", data_files_count, data_records);
    auto res = run_row_delta(table, std::move(data), {});
    ASSERT_FALSE(res.has_error()) << res.error();

    ASSERT_TRUE(table.snapshots.has_value());
    ASSERT_EQ(table.snapshots->size(), 1);
    const auto& snap = table.snapshots->back();
    ASSERT_EQ(snap.summary.operation, snapshot_operation::append);

    ASSERT_EQ(snap.summary.added_data_files, data_files_count);
    ASSERT_EQ(snap.summary.added_records, data_records);
    ASSERT_EQ(snap.summary.total_data_files, data_files_count);
    ASSERT_EQ(snap.summary.total_records, data_records);

    // Only data manifests in the list.
    auto mlist_res = io.download_manifest_list(snap.manifest_list_path).get();
    ASSERT_TRUE(mlist_res.has_value());
    const auto& mlist = mlist_res.value();
    ASSERT_EQ(mlist.files.size(), 1);
    ASSERT_EQ(mlist.files[0].content, manifest_file_content::data);
}
