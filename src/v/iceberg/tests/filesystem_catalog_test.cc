// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/datatypes.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/logger.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata_json.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/transform.h"
#include "json/document.h"

#include <gtest/gtest.h>

#include <memory>

using namespace iceberg;

class FileSystemCatalogTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    static constexpr std::string_view base_location{"test"};
    FileSystemCatalogTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location)) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    filesystem_catalog catalog;
};

TEST_F(FileSystemCatalogTest, TestLoadCreate) {
    const table_identifier id{.ns = {"ns"}, .table = "table"};

    // Loading the table before creation should fail.
    auto load_res = catalog.load_table(id).get();
    ASSERT_TRUE(load_res.has_error());
    ASSERT_EQ(load_res.error(), catalog::errc::not_found);

    table_io io(remote(), bucket_name);
    const auto expected_hint_path = version_hint_path{
      "test/ns/table/metadata/version-hint.text"};
    auto hint_exists_res = io.version_hint_exists(expected_hint_path).get();
    ASSERT_FALSE(hint_exists_res.has_error());
    ASSERT_FALSE(hint_exists_res.value());

    // Now create the table.
    auto create_res
      = catalog.create_table(id, schema{}, partition_spec{}).get();
    ASSERT_FALSE(create_res.has_error());

    // After creating, our hint should exist.
    hint_exists_res = io.version_hint_exists(expected_hint_path).get();
    ASSERT_FALSE(hint_exists_res.has_error());
    ASSERT_TRUE(hint_exists_res.value());

    // Sanity check the created file.
    const auto expected_meta_path = table_metadata_path{
      "test/ns/table/metadata/v0.metadata.json"};
    auto meta_res = io.download_table_meta(expected_meta_path).get();
    ASSERT_FALSE(meta_res.has_error());
    auto& created_meta = meta_res.value();
    ASSERT_EQ(created_meta, create_res.value());

    EXPECT_EQ(created_meta.current_schema_id, schema::id_t{0});
    EXPECT_EQ(created_meta.default_spec_id, partition_spec::id_t{0});
    EXPECT_EQ(1, created_meta.schemas.size());
    EXPECT_EQ(1, created_meta.partition_specs.size());
    EXPECT_EQ(1, created_meta.sort_orders.size());

    // Creating again should result in an error that the table exists.
    create_res = catalog.create_table(id, schema{}, partition_spec{}).get();
    ASSERT_TRUE(create_res.has_error());
    EXPECT_EQ(create_res.error(), catalog::errc::already_exists);

    // Reload the table and ensure it matches what we created.
    load_res = catalog.load_table(id).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_EQ(created_meta, load_res.value());
}

TEST_F(FileSystemCatalogTest, TestCommit) {
    const table_identifier id{.ns = {"ns"}, .table = "table"};
    auto create_res
      = catalog.create_table(id, schema{}, partition_spec{}).get();
    ASSERT_FALSE(create_res.has_error());
    manifest_io manifest_io(remote(), bucket_name);
    transaction txn(manifest_io, std::move(create_res.value()));
    auto set_schema_res = txn
                            .set_schema(schema{
                              .schema_struct = std::get<struct_type>(
                                test_nested_schema_type()),
                              .schema_id = schema::id_t{1},
                              .identifier_field_ids = {},
                            })
                            .get();
    ASSERT_FALSE(set_schema_res.has_error());

    table_io io(remote(), bucket_name);
    const auto v1_meta_path = table_metadata_path{
      "test/ns/table/metadata/v1.metadata.json"};
    {
        // The update hasn't been committed to the catalog yet so the metadata
        // shouldn't exist.
        auto meta_res = io.download_table_meta(v1_meta_path).get();
        ASSERT_TRUE(meta_res.has_error());
        EXPECT_EQ(meta_res.error(), metadata_io::errc::failed);

        auto load_res = catalog.load_table(id).get();
        ASSERT_FALSE(load_res.has_error());
        EXPECT_EQ(1, load_res.value().schemas.size());
        EXPECT_EQ(0, load_res.value().schemas.back().schema_id());
    }
    auto tx_res = catalog.commit_txn(id, std::move(txn)).get();
    ASSERT_FALSE(tx_res.has_error());

    // Check that the new metadata exists and is reflected in the catalog.
    auto meta_res = io.download_table_meta(v1_meta_path).get();
    ASSERT_FALSE(meta_res.has_error());

    auto load_res = catalog.load_table(id).get();
    ASSERT_FALSE(load_res.has_error());
    EXPECT_EQ(2, load_res.value().schemas.size());
    EXPECT_EQ(1, load_res.value().schemas.back().schema_id());

    // Now try committing a transaction to the wrong table.
    transaction another_txn(manifest_io, std::move(create_res.value()));
    set_schema_res = another_txn
                       .set_schema(schema{
                         .schema_struct = std::get<struct_type>(
                           test_nested_schema_type()),
                         .schema_id = schema::id_t{2},
                         .identifier_field_ids = {nested_field::id_t{1}},
                       })
                       .get();
    ASSERT_FALSE(set_schema_res.has_error());
    const table_identifier wrong_id{.ns = {"ns"}, .table = "who_me"};
    auto bad_tx_res
      = catalog.commit_txn(wrong_id, std::move(another_txn)).get();
    ASSERT_TRUE(bad_tx_res.has_error());
    ASSERT_EQ(bad_tx_res.error(), catalog::errc::not_found);

    // Check that the botched transaction didn't affect our metadata.
    load_res = catalog.load_table(id).get();
    ASSERT_FALSE(load_res.has_error());
    EXPECT_EQ(2, load_res.value().schemas.size());
    EXPECT_EQ(1, load_res.value().schemas.back().schema_id());

    const auto wrong_meta_path = table_metadata_path{
      "test/ns/who_me/metadata/v1.metadata.json"};
    const auto wrong_hint_path = table_metadata_path{
      "test/ns/who_me/metadata/version-hint.text"};
    meta_res = io.download_table_meta(wrong_meta_path).get();
    ASSERT_TRUE(meta_res.has_error());
    EXPECT_EQ(meta_res.error(), metadata_io::errc::failed);
    auto hint_exists_res = io.version_hint_exists(wrong_hint_path).get();
    ASSERT_FALSE(hint_exists_res.has_error());
    ASSERT_FALSE(hint_exists_res.value());
}
