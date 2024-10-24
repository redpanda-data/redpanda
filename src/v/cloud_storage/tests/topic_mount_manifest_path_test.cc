// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/topic_mount_manifest_path.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_storage;

namespace {
const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
} // namespace

TEST(TestTopicMountManifestPath, RoundTrip) {
    auto tp_ns = model::topic_namespace(model::ns("ns"), model::topic("tp"));
    auto path = topic_mount_manifest_path(
      test_uuid, tp_ns, model::initial_revision_id(42));
    ASSERT_EQ(path.prefix(), "migration/");
    ASSERT_EQ(path.cluster_uuid(), test_uuid);
    ASSERT_EQ(path.tp_ns(), tp_ns);
    ASSERT_EQ(path.rev(), model::initial_revision_id(42));

    const auto str = ss::sstring(path);
    ASSERT_EQ(str, "migration/deadbeef-0000-0000-0000-000000000000/ns/tp/42");

    auto parsed = topic_mount_manifest_path::parse(str);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(*parsed, path);

    ASSERT_EQ(str, ss::sstring(*parsed));
}

// Test that we return empty parse result for invalid topic mount manifest
// paths and don't fail with unexpected exceptions.
TEST(TestTopicMountManifestPath, ParsingBogusPaths) {
    for (const auto& path : {
           "foobarbaz",
           "migration",
           "migration/",
           "migration/foo",
           "migration/foo/bar",
           "migration/foo/bar/baz",
           "migration/foo/bar/baz/qux",
           // Valid label/uuid but bogus revision (non-numerical). Include
           // cases for common pitfalls when parsing integers.
           "migration/deadbeef-0000-0000-0000-000000000000/kafka/tp/abc",
           "migration/deadbeef-0000-0000-0000-000000000000/kafka/tp/abc21",
           "migration/deadbeef-0000-0000-0000-000000000000/kafka/tp/21abc",
           // Valid label/uuid/revision but too many path components.
           "migration/deadbeef-0000-0000-0000-000000000000/kafka/tp/21/qux",
         }) {
        const auto tmp = topic_mount_manifest_path::parse(path);
        ASSERT_FALSE(tmp.has_value()) << fmt::format(
          R"(Expected to fail parsing "{}" but parsed as "{}")",
          path,
          ss::sstring(*tmp));
    }
}
