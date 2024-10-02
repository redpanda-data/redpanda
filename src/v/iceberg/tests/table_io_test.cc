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
#include "iceberg/table_io.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_metadata_json.h"
#include "iceberg/tests/test_table_metadata.h"
#include "json/document.h"

#include <gtest/gtest.h>

#include <limits>

using namespace iceberg;

namespace {
table_metadata make_table_meta() {
    json::Document j;
    j.Parse(ss::sstring(test_table_meta_json));
    return parse_table_meta(j);
}
} // namespace

class TableIOTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    TableIOTest()
      : sr(cloud_io::scoped_remote::create(10, conf)) {
        set_expectations_and_listen({});
    }
    auto& remote() { return sr->remote.local(); }
    std::unique_ptr<cloud_io::scoped_remote> sr;
};

TEST_F(TableIOTest, TestTableMetadataRoundtrip) {
    const auto m = make_table_meta();
    auto io = table_io(remote(), bucket_name);
    const auto test_path = table_metadata_path{"foo/bar/baz"};

    // First it's missing.
    auto dl_res = io.download_table_meta(test_path).get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), metadata_io::errc::failed);

    // Now upload, then try again with success.
    auto ul_res = io.upload_table_meta(test_path, m).get();
    ASSERT_TRUE(ul_res.has_value());
    ASSERT_LT(0, ul_res.value());

    dl_res = io.download_table_meta(test_path).get();
    ASSERT_FALSE(dl_res.has_error());
    const auto& m_roundtrip = dl_res.value();
    ASSERT_EQ(m, m_roundtrip);
}

TEST_F(TableIOTest, TestVersionHintRoundTrip) {
    auto io = table_io(remote(), bucket_name);
    const auto test_path = "foo/bar/baz";
    std::vector<int> values{
      0,
      std::numeric_limits<int>::max(),
      std::numeric_limits<int>::min(),
    };
    for (size_t i = 0; i < values.size(); ++i) {
        auto v = values[i];
        auto path = version_hint_path{fmt::format("{}-{}", test_path, i)};
        auto up_res = io.upload_version_hint(path, v).get();
        ASSERT_FALSE(up_res.has_error());
        auto dl_res = io.download_version_hint(path).get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(v, dl_res.value());
    }
}

TEST_F(TableIOTest, TestInvalidVersionHint) {
    auto io = table_io(remote(), bucket_name);
    const auto test_path = version_hint_path{"foo/bar/baz"};
    add_expectations({expectation{
      .url = test_path().native(),
      .body = "100000000000",
    }});
    auto dl_res = io.download_version_hint(test_path).get();
    ASSERT_TRUE(dl_res.has_error());
}

TEST_F(TableIOTest, TestVersionHintCheckExists) {
    auto io = table_io(remote(), bucket_name);
    const auto test_path = version_hint_path{"foo/bar/baz"};
    auto ex_res = io.version_hint_exists(test_path).get();
    ASSERT_FALSE(ex_res.has_error());
    ASSERT_FALSE(ex_res.value());

    auto up_res = io.upload_version_hint(test_path, 1).get();
    ASSERT_FALSE(up_res.has_error());

    ex_res = io.version_hint_exists(test_path).get();
    ASSERT_FALSE(ex_res.has_error());
    ASSERT_TRUE(ex_res.value());
}
