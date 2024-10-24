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
#include "cloud_io/transfer_details.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/manifest_io.h"
#include "iceberg/manifest_list.h"
#include "iceberg/tests/test_schemas.h"
#include "utils/retry_chain_node.h"

#include <gtest/gtest.h>

using namespace iceberg;
using namespace std::chrono_literals;

partition_key_type empty_pk_type() { return partition_key_type{struct_type{}}; }

class ManifestIOTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    ManifestIOTest()
      : sr(cloud_io::scoped_remote::create(10, conf)) {
        set_expectations_and_listen({});
    }
    auto& remote() { return sr->remote.local(); }
    manifest make_manifest() const {
        schema s{
          .schema_struct = std::get<struct_type>(test_nested_schema_type()),
          .schema_id = schema::id_t{12},
          .identifier_field_ids = {nested_field::id_t{1}},
        };
        partition_spec p{
      .spec_id = partition_spec::id_t{8},
        .fields = {
            partition_field{
              .source_id = nested_field::id_t{2},
              .field_id = partition_field::id_t{1000},
              .name = "p0",
              .transform = bucket_transform{10},
            },
        },
    };
        manifest_metadata meta{
          .schema = std::move(s),
          .partition_spec = std::move(p),
          .format_version = format_version::v1,
          .manifest_content_type = manifest_content_type::data,
        };
        return manifest{
          .metadata = std::move(meta),
          .entries = {},
        };
    }

    manifest_list make_manifest_list() const {
        manifest_list m;
        for (int i = 0; i < 1024; i++) {
            manifest_file file;
            file.manifest_path = "path/to/file";
            file.partition_spec_id = partition_spec::id_t{1};
            file.content = manifest_file_content::data;
            file.seq_number = sequence_number{3};
            file.min_seq_number = sequence_number{4};
            file.added_snapshot_id = snapshot_id{5};
            file.added_files_count = 6;
            file.existing_files_count = 7;
            file.deleted_files_count = 8;
            file.added_rows_count = 9;
            file.existing_rows_count = 10;
            file.deleted_rows_count = 11;
            m.files.emplace_back(std::move(file));
        }
        return m;
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
};

TEST_F(ManifestIOTest, TestManifestRoundtrip) {
    auto m = make_manifest();
    // Missing manifest.
    auto io = manifest_io(remote(), bucket_name);
    auto test_path = manifest_path{"foo/bar/baz"};
    auto dl_res = io.download_manifest(test_path, empty_pk_type()).get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), metadata_io::errc::failed);

    // Now upload, then try again with success.
    auto ul_res = io.upload_manifest(test_path, m).get();
    ASSERT_TRUE(ul_res.has_value());
    ASSERT_LT(0, ul_res.value());

    dl_res = io.download_manifest(test_path, empty_pk_type()).get();
    ASSERT_FALSE(dl_res.has_error());
    const auto& m_roundtrip = dl_res.value();
    ASSERT_EQ(m, m_roundtrip);
}

TEST_F(ManifestIOTest, TestManifestListRoundtrip) {
    manifest_list m = make_manifest_list();

    // Missing manifest list.
    auto io = manifest_io(remote(), bucket_name);
    auto test_path = manifest_path{"foo/bar/baz"};
    auto dl_res = io.download_manifest_list(test_path).get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), metadata_io::errc::failed);

    // Now upload, then try again with success.
    auto ul_err = io.upload_manifest_list(test_path, m).get();
    ASSERT_FALSE(ul_err.has_error());

    dl_res = io.download_manifest_list(test_path).get();
    ASSERT_FALSE(dl_res.has_error());
    const auto& m_roundtrip = dl_res.value();
    ASSERT_EQ(m, m_roundtrip);
}

TEST_F(ManifestIOTest, TestManifestRoundtripURIs) {
    auto m = make_manifest();
    auto io = manifest_io(remote(), bucket_name);
    auto path = manifest_path{"foo/bar/baz"};
    auto test_uri = io.to_uri(path);
    ASSERT_TRUE(test_uri.starts_with("s3://"));

    auto up_res = io.upload_manifest(path, m).get();
    ASSERT_FALSE(up_res.has_error());

    // Use the URI string.
    auto dl_res = io.download_manifest_uri(test_uri, empty_pk_type()).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(m, dl_res.value());

    // As a safety measure, we'll still parse the raw path if given.
    dl_res = io.download_manifest_uri(path().native(), empty_pk_type()).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(m, dl_res.value());
}

TEST_F(ManifestIOTest, TestManifestListRoundtripURIs) {
    auto m = make_manifest_list();
    auto io = manifest_io(remote(), bucket_name);
    auto path = manifest_list_path{"foo/bar/baz"};
    auto test_uri = io.to_uri(path);
    ASSERT_TRUE(test_uri.starts_with("s3://"));

    auto up_res = io.upload_manifest_list(path, m).get();
    ASSERT_FALSE(up_res.has_error());

    // Use the URI string.
    auto dl_res = io.download_manifest_list_uri(test_uri).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(m, dl_res.value());

    // As a safety measure, we'll still parse the raw path if given.
    dl_res = io.download_manifest_list_uri(path().native()).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(m, dl_res.value());
}

TEST_F(ManifestIOTest, TestShutdown) {
    auto test_path = manifest_path{"foo/bar/baz"};
    sr->request_stop();
    auto io = manifest_io(remote(), bucket_name);
    {
        auto dl_res = io.download_manifest(test_path, empty_pk_type()).get();
        ASSERT_TRUE(dl_res.has_error());
        ASSERT_EQ(dl_res.error(), metadata_io::errc::shutting_down);

        auto ul_err = io.upload_manifest(test_path, manifest{}).get();
        ASSERT_TRUE(ul_err.has_error());
        ASSERT_EQ(ul_err.error(), metadata_io::errc::shutting_down);
    }
    {
        auto dl_res = io.download_manifest_list(test_path).get();
        ASSERT_TRUE(dl_res.has_error());
        ASSERT_EQ(dl_res.error(), metadata_io::errc::shutting_down);

        auto ul_err = io.upload_manifest_list(test_path, manifest_list{}).get();
        ASSERT_TRUE(ul_err.has_error());
        ASSERT_EQ(ul_err.error(), metadata_io::errc::shutting_down);
    }
}

TEST_F(ManifestIOTest, TestCorruptedDownload) {
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    auto test_path = manifest_path{"foo/bar/baz"};
    auto ul_res = remote().upload_object({
      .transfer_details = cloud_io::transfer_details{
        .bucket = bucket_name,
        .key = cloud_storage_clients::object_key{test_path()},
        .parent_rtc = retry,
      },
      .display_str = "blob",
      .payload = iobuf::from("blob"),
    }).get();
    ASSERT_EQ(ul_res, cloud_io::upload_result::success);
    auto io = manifest_io(remote(), bucket_name);
    {
        auto dl_res = io.download_manifest(test_path, empty_pk_type()).get();
        ASSERT_TRUE(dl_res.has_error());
        ASSERT_EQ(dl_res.error(), metadata_io::errc::failed);
    }
    {
        auto dl_res = io.download_manifest_list(test_path).get();
        ASSERT_TRUE(dl_res.has_error());
        ASSERT_EQ(dl_res.error(), metadata_io::errc::failed);
    }
}
