// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_properties.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "storage/ntp_config.h"

#include <gtest/gtest.h>

#include <memory>

namespace cluster {

// Avoid a common typo where a part of the string format is passed as an
// argument.
// clang-format off
// I.e fmt::format("a: {}", "b: {}", a, b) resulting in "a: b: {}"
// instead of fmt::format("a: {}, b: {}", a, b) which should result in "a: <value> b: <value>"
// clang-format on
TEST(TopicProperties, ostream) {
    topic_properties properties;
    std::ostringstream stream;
    stream << properties;
    auto result = stream.str();
    ASSERT_FALSE(result.contains("{}")) << result;
}

namespace {
storage::ntp_config make_ntp_config(
  model::redpanda_storage_mode mode, model::redpanda_storage_mode migrated) {
    storage::ntp_config::default_overrides o;
    o.storage_mode = mode;
    o.migrated_from = migrated;
    return {
      model::ntp(
        model::kafka_namespace, model::topic("t"), model::partition_id(0)),
      "",
      std::make_unique<storage::ntp_config::default_overrides>(o)};
}
} // namespace

// Tests cloud_topic_enabled/migrated_to_cloud behavior
// w.r.t. storage.mode and migrated_from
TEST(TopicProperties, ntp_config_migrated_to_cloud_matrix) {
    using mode = model::redpanda_storage_mode;

    storage::ntp_config bare(
      model::ntp(
        model::kafka_namespace, model::topic("t"), model::partition_id(0)),
      "");
    EXPECT_FALSE(bare.cloud_topic_enabled());
    EXPECT_FALSE(bare.migrated_to_cloud());

    for (auto m : {mode::local, mode::tiered, mode::unset}) {
        auto cfg = make_ntp_config(m, mode::unset);
        EXPECT_FALSE(cfg.cloud_topic_enabled());
        EXPECT_FALSE(cfg.migrated_to_cloud());
    }
    for (auto m : {mode::cloud, mode::tiered_cloud}) {
        auto native = make_ntp_config(m, mode::unset);
        EXPECT_TRUE(native.cloud_topic_enabled());
        EXPECT_FALSE(native.migrated_to_cloud());
        for (auto src : {mode::tiered, mode::local}) {
            auto migrated = make_ntp_config(m, src);
            EXPECT_TRUE(migrated.cloud_topic_enabled());
            EXPECT_TRUE(migrated.migrated_to_cloud());
        }
    }
}

} // namespace cluster
