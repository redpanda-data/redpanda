// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/topic_mount_manifest.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/randoms.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_storage;

namespace {
const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
const remote_label test_label{test_uuid};
const model::topic_namespace test_tp_ns_override{
  model::ns{"kafka"}, model::topic{"override"}};
const model::topic_namespace test_tp_ns{model::ns{"kafka"}, model::topic{"tp"}};
const model::initial_revision_id test_rev{21};
const model::partition_id test_pid{5};
const model::ntp test_ntp{test_tp_ns.ns, test_tp_ns.tp, test_pid};
const spillover_manifest_path_components test_spill_comps{
  .base = model::offset{10},
  .last = model::offset{11},
  .base_kafka = kafka::offset{0},
  .next_kafka = kafka::offset{1},
  .base_ts = model::timestamp{999},
  .last_ts = model::timestamp{1000},
};
const segment_meta test_smeta{
  .size_bytes = 1_MiB,
  .base_offset = test_spill_comps.base,
  .committed_offset = test_spill_comps.last,
  .base_timestamp = test_spill_comps.base_ts,
  .max_timestamp = test_spill_comps.last_ts,
  .delta_offset = model::offset_delta(10),
  .ntp_revision = test_rev,
  .archiver_term = model::term_id{13},
  .segment_term = model::term_id{12},
  .delta_offset_end = model::offset_delta(11),
  .sname_format = segment_name_format::v3,
};
} // namespace

// Parameterized topic namespace override with a label.
// For use with Labeled tests
class OverrideParamRemotePathProviderWithLabelTest
  : public ::testing::TestWithParam<bool> {
public:
    OverrideParamRemotePathProviderWithLabelTest()
      : has_override(GetParam())
      , path_provider(
          test_label,
          has_override
            ? std::make_optional<model::topic_namespace>(test_tp_ns_override)
            : std::nullopt) {}

protected:
    const bool has_override;
    const remote_path_provider path_provider;
};

// Parameterized topic namespace override without a label.
// For use with Labeled tests
class OverrideParamRemotePathProviderTest
  : public ::testing::TestWithParam<bool> {
public:
    OverrideParamRemotePathProviderTest()
      : has_override(GetParam())
      , path_provider(
          std::nullopt,
          has_override
            ? std::make_optional<model::topic_namespace>(test_tp_ns_override)
            : std::nullopt) {}

protected:
    const bool has_override;
    const remote_path_provider path_provider;
};

TEST_P(
  OverrideParamRemotePathProviderWithLabelTest,
  TestTopicFromLabeledTopicManifestPath) {
    auto bin_path = path_provider.topic_manifest_path(test_tp_ns, test_rev);
    auto parsed_labeled_tp_ns = tp_ns_from_labeled_path(bin_path);
    ASSERT_TRUE(parsed_labeled_tp_ns.has_value());
    const auto& expected_tp_ns = has_override ? test_tp_ns_override
                                              : test_tp_ns;
    ASSERT_EQ(*parsed_labeled_tp_ns, expected_tp_ns);

    // Using the wrong method should result in nullopt.
    auto parsed_prefixed_tp_ns = tp_ns_from_prefixed_path(bin_path);
    ASSERT_FALSE(parsed_prefixed_tp_ns.has_value());
}

TEST_P(
  OverrideParamRemotePathProviderTest, TestTopicFromPrefixedTopicManifestPath) {
    auto bin_path = path_provider.topic_manifest_path(test_tp_ns, test_rev);
    auto json_path = *path_provider.topic_manifest_path_json(test_tp_ns);

    auto parsed_bin_tp_ns = tp_ns_from_prefixed_path(bin_path);
    ASSERT_TRUE(parsed_bin_tp_ns.has_value());
    const auto& expected_tp_ns = has_override ? test_tp_ns_override
                                              : test_tp_ns;

    ASSERT_EQ(*parsed_bin_tp_ns, expected_tp_ns);

    auto parsed_json_tp_ns = tp_ns_from_prefixed_path(json_path);
    ASSERT_TRUE(parsed_json_tp_ns.has_value());
    ASSERT_EQ(*parsed_json_tp_ns, expected_tp_ns);

    // Using the wrong method should result in nullopt.
    auto parsed_labeled_tp_ns = tp_ns_from_labeled_path(bin_path);
    ASSERT_FALSE(parsed_labeled_tp_ns.has_value());
    parsed_labeled_tp_ns = tp_ns_from_labeled_path(json_path);
    ASSERT_FALSE(parsed_labeled_tp_ns.has_value());
}

TEST_P(OverrideParamRemotePathProviderTest, TestPrefixedTopicManifestPaths) {
    const ss::sstring expected_path = has_override
                                        ? "10000000/meta/kafka/override"
                                        : "e0000000/meta/kafka/tp";
    EXPECT_STREQ(
      path_provider.topic_manifest_path(test_tp_ns, test_rev).c_str(),
      fmt::format("{}/topic_manifest.bin", expected_path).c_str());
    EXPECT_STREQ(
      path_provider.topic_manifest_prefix(test_tp_ns).c_str(),
      expected_path.c_str());
    const auto json_str = path_provider.topic_manifest_path_json(test_tp_ns);
    ASSERT_TRUE(json_str.has_value());
    EXPECT_STREQ(
      json_str->c_str(),
      fmt::format("{}/topic_manifest.json", expected_path).c_str());
}

TEST_P(
  OverrideParamRemotePathProviderWithLabelTest, TestLabeledTopicManifestPaths) {
    const ss::sstring expected_path = has_override ? "meta/kafka/override"
                                                   : "meta/kafka/tp";
    EXPECT_STREQ(
      path_provider.topic_manifest_path(test_tp_ns, test_rev).c_str(),
      fmt::format(
        "{}/deadbeef-0000-0000-0000-000000000000/21/topic_manifest.bin",
        expected_path)
        .c_str());
    EXPECT_STREQ(
      path_provider.topic_manifest_prefix(test_tp_ns).c_str(),
      fmt::format("{}/deadbeef-0000-0000-0000-000000000000", expected_path)
        .c_str());

    // We don't expect to read or write JSON topic manifests with the cluster
    // uuid labels.
    const auto json_str = path_provider.topic_manifest_path_json(test_tp_ns);
    ASSERT_FALSE(json_str.has_value());
}

TEST_P(
  OverrideParamRemotePathProviderTest, TestPrefixedPartitionManifestPaths) {
    const ss::sstring expected_path = has_override
                                        ? "50000000/meta/kafka/override"
                                        : "e0000000/meta/kafka/tp";
    EXPECT_STREQ(
      path_provider.partition_manifest_path(test_ntp, test_rev).c_str(),
      fmt::format("{}/5_21/manifest.bin", expected_path).c_str());
    EXPECT_STREQ(
      path_provider.partition_manifest_prefix(test_ntp, test_rev).c_str(),
      fmt::format("{}/5_21", expected_path).c_str());

    auto json_path = path_provider.partition_manifest_path_json(
      test_ntp, test_rev);
    ASSERT_TRUE(json_path.has_value());
    EXPECT_STREQ(
      json_path.value().c_str(),
      fmt::format("{}/5_21/manifest.json", expected_path).c_str());

    partition_manifest pm(test_ntp, test_rev);
    EXPECT_STREQ(
      path_provider.partition_manifest_path(pm).c_str(),
      fmt::format("{}/5_21/manifest.bin", expected_path).c_str());
    EXPECT_STREQ(
      path_provider.spillover_manifest_path(pm, test_spill_comps).c_str(),
      fmt::format("{}/5_21/manifest.bin.10.11.0.1.999.1000", expected_path)
        .c_str());
    EXPECT_STREQ(
      pm.get_manifest_path(path_provider)().native().c_str(),
      fmt::format("{}/5_21/manifest.bin", expected_path).c_str());

    spillover_manifest spill_m(test_ntp, test_rev);
    spill_m.add(test_smeta);
    EXPECT_STREQ(
      path_provider.partition_manifest_path(spill_m).c_str(),
      fmt::format("{}/5_21/manifest.bin.10.11.0.1.999.1000", expected_path)
        .c_str());
    EXPECT_STREQ(
      spill_m.get_manifest_path(path_provider)().native().c_str(),
      fmt::format("{}/5_21/manifest.bin.10.11.0.1.999.1000", expected_path)
        .c_str());
    partition_manifest* disguised_manifest = &spill_m;
    EXPECT_STREQ(
      disguised_manifest->get_manifest_path(path_provider)().native().c_str(),
      fmt::format("{}/5_21/manifest.bin.10.11.0.1.999.1000", expected_path)
        .c_str());
}

TEST_P(
  OverrideParamRemotePathProviderWithLabelTest,
  TestLabeledPartitionManifestPaths) {
    const ss::sstring expected_path = has_override ? "meta/kafka/override"
                                                   : "meta/kafka/tp";
    EXPECT_STREQ(
      path_provider.partition_manifest_path(test_ntp, test_rev).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/manifest.bin",
        expected_path)
        .c_str());
    EXPECT_STREQ(
      path_provider.partition_manifest_prefix(test_ntp, test_rev).c_str(),
      fmt::format("deadbeef-0000-0000-0000-000000000000/{}/5_21", expected_path)
        .c_str());

    auto json_path = path_provider.partition_manifest_path_json(
      test_ntp, test_rev);
    ASSERT_FALSE(json_path.has_value());

    partition_manifest pm(test_ntp, test_rev);
    EXPECT_STREQ(
      path_provider.partition_manifest_path(pm).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/manifest.bin",
        expected_path)
        .c_str());
    EXPECT_STREQ(
      pm.get_manifest_path(path_provider)().native().c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/manifest.bin",
        expected_path)
        .c_str());
    EXPECT_STREQ(
      path_provider.spillover_manifest_path(pm, test_spill_comps).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "manifest.bin.10.11.0.1.999.1000",
        expected_path)
        .c_str());

    spillover_manifest spill_m(test_ntp, test_rev);
    spill_m.add(test_smeta);
    EXPECT_STREQ(
      path_provider.partition_manifest_path(spill_m).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "manifest.bin.10.11.0.1.999.1000",
        expected_path)
        .c_str());
    EXPECT_STREQ(
      spill_m.get_manifest_path(path_provider)().native().c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "manifest.bin.10.11.0.1.999.1000",
        expected_path)
        .c_str());
    partition_manifest* disguised_manifest = &spill_m;
    EXPECT_STREQ(
      disguised_manifest->get_manifest_path(path_provider)().native().c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "manifest.bin.10.11.0.1.999.1000",
        expected_path)
        .c_str());
}

TEST_P(OverrideParamRemotePathProviderTest, TestPrefixedSegmentPaths) {
    const ss::sstring expected_path = has_override ? "426917a1/kafka/override"
                                                   : "d13f1c8e/kafka/tp";
    partition_manifest pm(test_ntp, test_rev);
    EXPECT_STREQ(
      path_provider.segment_path(pm, test_smeta).c_str(),
      fmt::format("{}/5_21/10-11-1048576-12-v1.log.13", expected_path).c_str());
    EXPECT_STREQ(
      path_provider.segment_path(test_ntp, test_rev, test_smeta).c_str(),
      fmt::format("{}/5_21/10-11-1048576-12-v1.log.13", expected_path).c_str());

    // Resetting the term should result in the term being missing.
    auto smeta_no_term = test_smeta;
    smeta_no_term.archiver_term = model::term_id{};
    EXPECT_STREQ(
      path_provider.segment_path(pm, smeta_no_term).c_str(),
      fmt::format("{}/5_21/10-11-1048576-12-v1.log", expected_path).c_str());
}

TEST_P(OverrideParamRemotePathProviderWithLabelTest, TestLabeledSegmentPaths) {
    const ss::sstring expected_tp_ns = has_override ? "kafka/override"
                                                    : "kafka/tp";
    partition_manifest pm(test_ntp, test_rev);
    EXPECT_STREQ(
      path_provider.segment_path(pm, test_smeta).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "10-11-1048576-12-v1.log.13",
        expected_tp_ns)
        .c_str());
    EXPECT_STREQ(
      path_provider.segment_path(test_ntp, test_rev, test_smeta).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "10-11-1048576-12-v1.log.13",
        expected_tp_ns)
        .c_str());

    // Resetting the term should result in the term being missing.
    auto smeta_no_term = test_smeta;
    smeta_no_term.archiver_term = model::term_id{};
    EXPECT_STREQ(
      path_provider.segment_path(pm, smeta_no_term).c_str(),
      fmt::format(
        "deadbeef-0000-0000-0000-000000000000/{}/5_21/"
        "10-11-1048576-12-v1.log",
        expected_tp_ns)
        .c_str());
}

class LabelParamRemotePathProviderTest : public ::testing::TestWithParam<bool> {
public:
    LabelParamRemotePathProviderTest()
      : path_provider(
          GetParam() ? std::make_optional<remote_label>(
                         model::cluster_uuid{uuid_t::create()})
                     : std::nullopt,
          std::nullopt) {}

protected:
    const remote_path_provider path_provider;
};

TEST_P(LabelParamRemotePathProviderTest, TestTopicPrefixPrefixesPath) {
    // The topic manifest prefix, if used as a list prefix, should catch the
    // topic manifest.
    const auto topic_path = path_provider.topic_manifest_path(
      test_tp_ns, test_rev);
    const auto topic_prefix = path_provider.topic_manifest_prefix(test_tp_ns);
    ASSERT_TRUE(topic_path.starts_with(topic_prefix));
}

TEST_P(LabelParamRemotePathProviderTest, TestPartitionPrefixPrefixesPath) {
    // The partition manifest prefix, if used as a list prefix, should catch
    // both STM manifests and spillover manifests.
    const auto partition_path = path_provider.partition_manifest_path(
      test_ntp, test_rev);
    const auto partition_prefix = path_provider.partition_manifest_prefix(
      test_ntp, test_rev);
    ASSERT_TRUE(partition_path.starts_with(partition_prefix));

    partition_manifest pm(test_ntp, test_rev);
    const auto spillover_path = path_provider.spillover_manifest_path(
      pm, test_spill_comps);
    ASSERT_TRUE(spillover_path.starts_with(partition_prefix));
}

TEST_P(
  LabelParamRemotePathProviderTest, TestPartitionPrefixDoesntPrefixSegments) {
    // The partition manifest prefix, if used as a list prefix, shouldn't catch
    // any segments.
    const auto partition_prefix = path_provider.partition_manifest_prefix(
      test_ntp, test_rev);
    ASSERT_FALSE(path_provider.segment_path(test_ntp, test_rev, test_smeta)
                   .starts_with(partition_prefix));
}

TEST(RemotePathProviderTest, TestTopicMountManifestPath) {
    remote_path_provider path_provider(test_label, std::nullopt);
    topic_mount_manifest manifest(test_label, test_tp_ns, test_rev);
    EXPECT_STREQ(
      path_provider.topic_mount_manifest_path(manifest).c_str(),
      "migration/deadbeef-0000-0000-0000-000000000000/kafka/tp/21");
}

INSTANTIATE_TEST_SUITE_P(
  LabeledWithOverride,
  OverrideParamRemotePathProviderWithLabelTest,
  ::testing::Bool());

INSTANTIATE_TEST_SUITE_P(
  WithOverride, OverrideParamRemotePathProviderTest, ::testing::Bool());

INSTANTIATE_TEST_SUITE_P(
  WithLabel, LabelParamRemotePathProviderTest, ::testing::Bool());
