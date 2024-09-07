// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/manifest_io.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace iceberg;

class ManifestIOTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    ManifestIOTest()
      : sr(cloud_io::scoped_remote::create(10, conf)) {
        set_expectations_and_listen({});
    }
    auto& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;
};

TEST_F(ManifestIOTest, TestRoundtrip) {
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
    manifest m{
      .metadata = std::move(meta),
      .entries = {},
    };

    // Missing manifest.
    auto io = manifest_io(remote(), bucket_name);
    auto test_path = manifest_path{"foo/bar/baz"};
    auto dl_res = io.download_manifest(test_path).get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), manifest_io::errc::failed);

    // Now upload, then try again with success.
    auto ul_err = io.upload_manifest(test_path, m).get();
    ASSERT_FALSE(ul_err.has_value());

    dl_res = io.download_manifest(test_path).get();
    ASSERT_FALSE(dl_res.has_error());
    const auto& m_roundtrip = dl_res.value();
    ASSERT_EQ(m.metadata, m_roundtrip.metadata);
    // TODO: once the manifest stop using avrogen:: types, compare the
    // manifests directly.
    ASSERT_EQ(0, m_roundtrip.entries.size());
}

TEST_F(ManifestIOTest, TestShutdown) {
    auto test_path = manifest_path{"foo/bar/baz"};
    sr->request_stop();
    auto io = manifest_io(remote(), bucket_name);
    auto dl_res = io.download_manifest(test_path).get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), manifest_io::errc::shutting_down);

    auto ul_err = io.upload_manifest(test_path, manifest{}).get();
    ASSERT_TRUE(ul_err.has_value());
    ASSERT_EQ(ul_err.value(), manifest_io::errc::shutting_down);
}
