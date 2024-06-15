// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/remote_path_provider.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_storage;

namespace {
const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
const remote_label test_label{test_uuid};
const model::topic_namespace test_tp_ns{model::ns{"kafka"}, model::topic{"tp"}};
const model::initial_revision_id test_rev{21};
} // namespace

TEST(RemotePathProviderTest, TestPrefixedTopicManifestPaths) {
    remote_path_provider path_provider(std::nullopt);
    EXPECT_STREQ(
      path_provider.topic_manifest_path(test_tp_ns, test_rev).c_str(),
      "e0000000/meta/kafka/tp/topic_manifest.bin");
    EXPECT_STREQ(
      path_provider.topic_manifest_prefix(test_tp_ns).c_str(),
      "e0000000/meta/kafka/tp");
}

TEST(RemotePathProviderTest, TestLabeledTopicManifestPaths) {
    remote_path_provider path_provider(test_label);
    EXPECT_STREQ(
      path_provider.topic_manifest_path(test_tp_ns, test_rev).c_str(),
      "meta/kafka/tp/deadbeef-0000-0000-0000-000000000000/21/"
      "topic_manifest.bin");
    EXPECT_STREQ(
      path_provider.topic_manifest_prefix(test_tp_ns).c_str(),
      "meta/kafka/tp/deadbeef-0000-0000-0000-000000000000");
}

class ParamsRemotePathProviderTest : public ::testing::TestWithParam<bool> {
public:
    ParamsRemotePathProviderTest()
      : path_provider(
        GetParam() ? std::make_optional<remote_label>(
          model::cluster_uuid{uuid_t::create()})
                   : std::nullopt) {}

protected:
    const remote_path_provider path_provider;
};

TEST_P(ParamsRemotePathProviderTest, TestTopicPrefixPrefixesPath) {
    // The topic manifest prefix, if used as a list prefix, should catch the
    // topic manifest.
    const auto topic_path = path_provider.topic_manifest_path(
      test_tp_ns, test_rev);
    const auto topic_prefix = path_provider.topic_manifest_prefix(test_tp_ns);
    ASSERT_TRUE(topic_path.starts_with(topic_prefix));
}

INSTANTIATE_TEST_SUITE_P(
  WithLabel, ParamsRemotePathProviderTest, ::testing::Bool());
