// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/uri.h"
#include "test_utils/test.h"

using namespace iceberg;

TEST(IcebergUriTest, TestGettingUriPath) {
    uri valid_uri("s3://bucket-name/foo/bar/baz/manifest.avro");

    auto path = path_from_uri(valid_uri);

    ASSERT_EQ(path, "foo/bar/baz/manifest.avro");
}

TEST(IcebergUriTest, TestParsingInvalidURI) {
    uri valid_uri("/bucket-name/foo/bar/baz/manifest.avro");

    ASSERT_THROW(path_from_uri(valid_uri), std::invalid_argument);
}

TEST(IcebergUriTest, TestConstructingUriFromParts) {
    auto uri = make_uri(
      "panda-bucket",
      std::filesystem::path("foo/bar/baz/element/from/path/test.json"));

    ASSERT_EQ(
      uri,
      iceberg::uri(
        "s3://panda-bucket/foo/bar/baz/element/from/path/test.json"));
}
