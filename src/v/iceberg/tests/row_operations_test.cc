/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "container/chunked_hash_map.h"
#include "iceberg/manifest_io.h"
#include "iceberg/merge_append_action.h"
#include "iceberg/partition.h"
#include "iceberg/row_operations.h"
#include "iceberg/table_update.h"
#include "iceberg/table_update_applier.h"
#include "iceberg/values.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {

serde::parquet::schema_element two_int64_schema() {
    using serde::parquet::field_repetition_type;
    using serde::parquet::i64_type;
    using serde::parquet::schema_element;

    chunked_vector<schema_element> children;
    children.push_back(
      schema_element{
        .type = i64_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("id")},
        .field_id = 1,
      });
    children.push_back(
      schema_element{
        .type = i64_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("val")},
        .field_id = 2,
      });
    return schema_element{
      .type = std::monostate{},
      .repetition_type = field_repetition_type::required,
      .path = {ss::sstring("data")},
      .children = std::move(children),
    };
}

serde::parquet::group_value make_row(int64_t id, int64_t val) {
    serde::parquet::group_value row;
    row.push_back(
      serde::parquet::group_member{serde::parquet::int64_value{id}});
    row.push_back(
      serde::parquet::group_member{serde::parquet::int64_value{val}});
    return row;
}

iobuf write_data_file(chunked_vector<serde::parquet::group_value> rows) {
    iobuf file;
    serde::parquet::writer w(
      {.schema = two_int64_schema()}, make_iobuf_ref_output_stream(file));
    w.init().get();
    for (auto& row : rows) {
        w.write_row(std::move(row)).get();
    }
    w.close().get();
    return file;
}

} // namespace

// NOLINTBEGIN(*magic-number*)

class RowOperationsTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    RowOperationsTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , io(remote(), bucket_name) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    table_metadata create_table() {
        struct_type st;
        st.fields.push_back(
          nested_field::create(1, "id", field_required::yes, long_type{}));
        st.fields.push_back(
          nested_field::create(2, "val", field_required::yes, long_type{}));

        auto s = schema{
          .schema_struct = std::move(st),
          .schema_id = schema::id_t{0},
          .identifier_field_ids = {nested_field::id_t{1}},
        };
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());

        chunked_vector<partition_spec> pspecs;
        pspecs.emplace_back(
          partition_spec{
            .spec_id = partition_spec::id_t{0},
            .fields = {},
          });

        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = uri(fmt::format("s3://{}/test", bucket_name())),
          .last_sequence_number = sequence_number{0},
          .last_updated_ms = model::timestamp::now(),
          .last_column_id = nested_field::id_t{2},
          .schemas = std::move(schemas),
          .current_schema_id = schema::id_t{0},
          .partition_specs = std::move(pspecs),
          .default_spec_id = partition_spec::id_t{0},
          .last_partition_id = partition_field::id_t{-1},
        };
    }

    chunked_vector<file_to_append> make_files(
      const table_metadata& md,
      const ss::sstring& path_prefix,
      chunked_vector<chunked_vector<serde::parquet::group_value>> files_rows) {
        chunked_vector<file_to_append> result;
        for (size_t i = 0; i < files_rows.size(); ++i) {
            auto path = uri(fmt::format("{}-{}.parquet", path_prefix, i));
            auto num_rows = files_rows[i].size();
            auto file_data = write_data_file(std::move(files_rows[i]));
            auto file_size = file_data.size_bytes();
            file_store[path()] = std::move(file_data);

            data_file df{
              .content_type = data_file_content_type::data,
              .file_path = path,
              .file_format = data_file_format::parquet,
              .partition = partition_key{std::make_unique<struct_value>()},
              .record_count = num_rows,
              .file_size_bytes = file_size,
            };
            result.push_back(
              file_to_append{
                .file = std::move(df),
                .schema_id = md.current_schema_id,
                .partition_spec_id = md.default_spec_id,
              });
        }
        return result;
    }

    void
    commit_files(table_metadata& table, chunked_vector<file_to_append> files) {
        merge_append_action maa(io, table, std::move(files));
        action& a = maa;
        auto outcome = std::move(a).build_updates().get();
        ASSERT_FALSE(outcome.has_error()) << outcome.error();
        for (auto& u : outcome.value().updates) {
            auto res = table_update::apply(u, table);
            ASSERT_EQ(res, table_update::outcome::success);
        }
    }

    read_file_fn make_read_fn() {
        return [this](const uri& path) -> ss::future<iobuf> {
            ++read_file_calls;
            auto it = file_store.find(path());
            vassert(it != file_store.end(), "file not found: {}", path());
            return ss::make_ready_future<iobuf>(it->second.copy());
        };
    }

    chunked_vector<nested_field::id_t> key_on_id() {
        chunked_vector<nested_field::id_t> ids;
        ids.push_back(nested_field::id_t{1});
        return ids;
    }

    chunked_hash_set<ss::sstring>
    file_paths_from(const chunked_vector<file_to_append>& files) {
        chunked_hash_set<ss::sstring> paths;
        for (const auto& f : files) {
            paths.insert(f.file.file_path());
        }
        return paths;
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    manifest_io io;
    chunked_hash_map<ss::sstring, iobuf> file_store;
    int read_file_calls{0};
};

TEST_F(RowOperationsTest, NoExistingSnapshot) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> rows;
    chunked_vector<serde::parquet::group_value> f;
    f.push_back(make_row(1, 100));
    rows.push_back(std::move(f));

    auto new_files = make_files(table, "new", std::move(rows));
    auto exclude = file_paths_from(new_files);
    auto keys
      = extract_keys(table, new_files, key_on_id(), make_read_fn()).get();
    EXPECT_EQ(keys.size(), 1);

    auto positions = find_matching_positions(
                       table, io, keys, key_on_id(), exclude, make_read_fn())
                       .get();
    EXPECT_TRUE(positions.empty());
}

TEST_F(RowOperationsTest, OverlappingKeysProduceDeletes) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> existing;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(1, 10));
        f.push_back(make_row(2, 20));
        existing.push_back(std::move(f));
    }
    auto existing_files = make_files(table, "existing", std::move(existing));
    commit_files(table, std::move(existing_files));

    chunked_vector<chunked_vector<serde::parquet::group_value>> new_rows;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(2, 200));
        f.push_back(make_row(3, 300));
        new_rows.push_back(std::move(f));
    }
    auto new_files = make_files(table, "new", std::move(new_rows));
    auto exclude = file_paths_from(new_files);

    auto keys
      = extract_keys(table, new_files, key_on_id(), make_read_fn()).get();
    auto positions = find_matching_positions(
                       table, io, keys, key_on_id(), exclude, make_read_fn())
                       .get();
    auto deletes = make_position_deletes(table, std::move(positions)).get();

    ASSERT_EQ(deletes.size(), 1);
    EXPECT_EQ(
      deletes[0].file.file.content_type,
      data_file_content_type::position_deletes);
    EXPECT_EQ(deletes[0].file.file.record_count, 1);
    EXPECT_GT(deletes[0].data.size_bytes(), 0);
}

TEST_F(RowOperationsTest, NoOverlapNoDeletes) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> existing;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(10, 100));
        f.push_back(make_row(20, 200));
        existing.push_back(std::move(f));
    }
    auto existing_files = make_files(table, "existing", std::move(existing));
    commit_files(table, std::move(existing_files));

    chunked_vector<chunked_vector<serde::parquet::group_value>> new_rows;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(30, 300));
        f.push_back(make_row(40, 400));
        new_rows.push_back(std::move(f));
    }
    auto new_files = make_files(table, "new", std::move(new_rows));
    auto exclude = file_paths_from(new_files);

    auto keys
      = extract_keys(table, new_files, key_on_id(), make_read_fn()).get();
    auto positions = find_matching_positions(
                       table, io, keys, key_on_id(), exclude, make_read_fn())
                       .get();
    auto deletes = make_position_deletes(table, std::move(positions)).get();

    EXPECT_TRUE(deletes.empty());
}

TEST_F(RowOperationsTest, MultipleExistingFiles) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> existing;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(1, 10));
        f.push_back(make_row(2, 20));
        existing.push_back(std::move(f));
    }
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(3, 30));
        f.push_back(make_row(4, 40));
        existing.push_back(std::move(f));
    }
    auto existing_files = make_files(table, "existing", std::move(existing));
    commit_files(table, std::move(existing_files));

    chunked_vector<chunked_vector<serde::parquet::group_value>> new_rows;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(2, 200));
        f.push_back(make_row(4, 400));
        new_rows.push_back(std::move(f));
    }
    auto new_files = make_files(table, "new", std::move(new_rows));
    auto exclude = file_paths_from(new_files);

    auto keys
      = extract_keys(table, new_files, key_on_id(), make_read_fn()).get();
    auto positions = find_matching_positions(
                       table, io, keys, key_on_id(), exclude, make_read_fn())
                       .get();
    auto deletes = make_position_deletes(table, std::move(positions)).get();

    ASSERT_EQ(deletes.size(), 1);
    EXPECT_EQ(deletes[0].file.file.record_count, 2);
}

TEST_F(RowOperationsTest, ExtractKeysOnly) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> rows;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(10, 100));
        f.push_back(make_row(20, 200));
        f.push_back(make_row(30, 300));
        rows.push_back(std::move(f));
    }
    auto files = make_files(table, "data", std::move(rows));

    auto keys = extract_keys(table, files, key_on_id(), make_read_fn()).get();

    ASSERT_EQ(keys.size(), 3);
    // Each key should have exactly one member (the "id" column).
    for (const auto& k : keys) {
        ASSERT_EQ(k.size(), 1);
    }
    auto get_id = [](const serde::parquet::group_value& k) {
        return std::get<serde::parquet::int64_value>(k[0].field).val;
    };
    EXPECT_EQ(get_id(keys[0]), 10);
    EXPECT_EQ(get_id(keys[1]), 20);
    EXPECT_EQ(get_id(keys[2]), 30);
}

TEST_F(RowOperationsTest, FindMatchingWithExclusion) {
    auto table = create_table();

    // Commit a file with id=1, id=2.
    chunked_vector<chunked_vector<serde::parquet::group_value>> existing;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(1, 10));
        f.push_back(make_row(2, 20));
        existing.push_back(std::move(f));
    }
    auto existing_files = make_files(table, "existing", std::move(existing));
    auto existing_path = existing_files[0].file.file_path();
    commit_files(table, std::move(existing_files));

    // Build keys that match id=1.
    chunked_vector<serde::parquet::group_value> keys;
    {
        serde::parquet::group_value k;
        k.push_back(
          serde::parquet::group_member{serde::parquet::int64_value{1}});
        keys.push_back(std::move(k));
    }

    // Without exclusion, should find a match.
    chunked_hash_set<ss::sstring> no_exclude;
    auto positions = find_matching_positions(
                       table, io, keys, key_on_id(), no_exclude, make_read_fn())
                       .get();
    EXPECT_EQ(positions.size(), 1);

    // When we exclude the existing file, should find no matches.
    chunked_hash_set<ss::sstring> exclude;
    exclude.insert(existing_path);
    auto positions2 = find_matching_positions(
                        table, io, keys, key_on_id(), exclude, make_read_fn())
                        .get();
    EXPECT_TRUE(positions2.empty());
}

TEST_F(RowOperationsTest, MakeEqualityDeletes) {
    auto table = create_table();

    chunked_vector<serde::parquet::group_value> keys;
    {
        serde::parquet::group_value k;
        k.push_back(
          serde::parquet::group_member{serde::parquet::int64_value{42}});
        keys.push_back(std::move(k));
    }
    {
        serde::parquet::group_value k;
        k.push_back(
          serde::parquet::group_member{serde::parquet::int64_value{99}});
        keys.push_back(std::move(k));
    }

    auto deletes = make_equality_deletes(table, keys, key_on_id()).get();

    ASSERT_EQ(deletes.size(), 1);
    EXPECT_EQ(
      deletes[0].file.content_type, data_file_content_type::equality_deletes);
    EXPECT_EQ(deletes[0].file.record_count, 2);
    ASSERT_TRUE(deletes[0].file.equality_ids.has_value());
    ASSERT_EQ(deletes[0].file.equality_ids->size(), 1);
    EXPECT_EQ((*deletes[0].file.equality_ids)[0], nested_field::id_t{1});
}

TEST_F(RowOperationsTest, CompositionEqualityPath) {
    auto table = create_table();

    chunked_vector<chunked_vector<serde::parquet::group_value>> rows;
    {
        chunked_vector<serde::parquet::group_value> f;
        f.push_back(make_row(5, 50));
        f.push_back(make_row(6, 60));
        rows.push_back(std::move(f));
    }
    auto new_files = make_files(table, "new", std::move(rows));

    auto keys
      = extract_keys(table, new_files, key_on_id(), make_read_fn()).get();
    ASSERT_EQ(keys.size(), 2);

    auto deletes = make_equality_deletes(table, keys, key_on_id()).get();

    ASSERT_EQ(deletes.size(), 1);
    EXPECT_EQ(
      deletes[0].file.content_type, data_file_content_type::equality_deletes);
    EXPECT_EQ(deletes[0].file.record_count, 2);
}

// NOLINTEND(*magic-number*)
