/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/metadata.h"
#include "storage/ntp_config.h"

#include <gtest/gtest.h>

// The serving predicates key on partition_mode (the partition's own durable
// mode), not on topic_mode (what the topic config asks for). Every call site of
// these predicates is untouched by that split, so pinning the table here is
// what establishes that a partition mid tiered->cloud migration keeps being
// served as tiered storage until cutover.
namespace storage {
namespace {

using mode = model::redpanda_storage_mode;

ntp_config make_cfg(std::optional<mode> topic_mode) {
    auto overrides = std::make_unique<ntp_config::default_overrides>();
    if (topic_mode.has_value()) {
        overrides->storage_mode = *topic_mode;
    }
    return ntp_config{
      model::ntp{
        model::kafka_namespace, model::topic{"t"}, model::partition_id{0}},
      "/tmp/test",
      std::move(overrides)};
}

TEST(ntp_config_storage_mode, partition_mode_falls_back_to_topic_mode) {
    // Until the partition_properties STM records a mode, partition_mode is
    // `unset` and the accessors read through to the topic config.
    auto cfg = make_cfg(mode::tiered);
    EXPECT_EQ(cfg.topic_mode(), mode::tiered);
    EXPECT_EQ(cfg.partition_mode(), mode::tiered);
    EXPECT_TRUE(cfg.is_archival_enabled());
    EXPECT_TRUE(cfg.is_remote_fetch_enabled());
    EXPECT_FALSE(cfg.cloud_topic_enabled());
    EXPECT_FALSE(cfg.is_migrating());
}

TEST(ntp_config_storage_mode, native_cloud_topic) {
    auto cfg = make_cfg(mode::cloud);
    cfg.set_partition_mode(mode::cloud);
    EXPECT_TRUE(cfg.cloud_topic_enabled());
    EXPECT_FALSE(cfg.is_tiered_cloud());
    EXPECT_FALSE(cfg.is_archival_enabled());
    EXPECT_FALSE(cfg.is_remote_fetch_enabled());
    // Already the requested mode, so there is nothing to migrate.
    EXPECT_FALSE(cfg.is_migrating());
}

TEST(ntp_config_storage_mode, migrating_is_served_as_tiered_storage) {
    // The migration trigger: the topic config asks for cloud while the
    // partition's durable mode is still tiered. The partition must keep every
    // tiered-storage behavior -- archival uploads and remote fetch -- and must
    // NOT be routed to the cloud-topic path.
    for (auto destination : {mode::cloud, mode::tiered_cloud}) {
        auto cfg = make_cfg(destination);
        cfg.set_partition_mode(mode::tiered);
        EXPECT_EQ(cfg.topic_mode(), destination);
        EXPECT_EQ(cfg.partition_mode(), mode::tiered);
        EXPECT_TRUE(cfg.is_migrating());
        EXPECT_FALSE(cfg.cloud_topic_enabled());
        EXPECT_TRUE(cfg.is_archival_enabled());
        EXPECT_TRUE(cfg.is_remote_fetch_enabled());
    }
}

TEST(ntp_config_storage_mode, cutover_flips_routing) {
    // Cutover advances partition_mode to match topic_mode. That single write is
    // the routing flip: the partition stops being archival/remote-fetch and
    // becomes a cloud topic, and is no longer migrating.
    auto cfg = make_cfg(mode::tiered_cloud);
    cfg.set_partition_mode(mode::tiered);
    ASSERT_TRUE(cfg.is_migrating());

    cfg.set_partition_mode(mode::tiered_cloud);
    EXPECT_FALSE(cfg.is_migrating());
    EXPECT_TRUE(cfg.cloud_topic_enabled());
    EXPECT_TRUE(cfg.is_tiered_cloud());
    EXPECT_FALSE(cfg.is_archival_enabled());
    EXPECT_FALSE(cfg.is_remote_fetch_enabled());
}

TEST(ntp_config_storage_mode, unset_falls_back_to_shadow_indexing) {
    // Legacy topics carry no storage_mode; both modes read `unset` and the
    // accessors fall back to shadow_indexing_mode. The split must not disturb
    // this path.
    auto cfg = make_cfg(std::nullopt);
    ASSERT_EQ(cfg.topic_mode(), mode::unset);
    ASSERT_EQ(cfg.partition_mode(), mode::unset);
    EXPECT_FALSE(cfg.is_migrating());
    EXPECT_FALSE(cfg.cloud_topic_enabled());

    auto overrides = ntp_config::default_overrides{};
    overrides.shadow_indexing_mode = model::shadow_indexing_mode::full;
    cfg.set_overrides(overrides);
    EXPECT_TRUE(cfg.is_archival_enabled());
    EXPECT_TRUE(cfg.is_remote_fetch_enabled());

    overrides.shadow_indexing_mode = model::shadow_indexing_mode::disabled;
    cfg.set_overrides(overrides);
    EXPECT_FALSE(cfg.is_archival_enabled());
    EXPECT_FALSE(cfg.is_remote_fetch_enabled());
}

} // namespace
} // namespace storage
