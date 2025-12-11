/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/table_utils.h"

#include "cluster_link/model/types.h"
#include "ssx/async_algorithm.h"

namespace cluster::cluster_link {

using ::cluster_link::model::consumer_groups_mirroring_config;
using ::cluster_link::model::id_t;
using ::cluster_link::model::link_configuration;
using ::cluster_link::model::link_state;
using ::cluster_link::model::metadata;
using ::cluster_link::model::metadata_ptr;
using ::cluster_link::model::mirror_topic_metadata;
using ::cluster_link::model::security_settings_sync_config;
using ::cluster_link::model::topic_metadata_mirroring_config;

namespace {
mirror_topic_metadata
copy_mirror_topic_metadata(const mirror_topic_metadata& old) {
    mirror_topic_metadata copy;
    copy.status = old.status;
    copy.source_topic_id = old.source_topic_id;
    copy.source_topic_name = old.source_topic_name;
    copy.destination_topic_id = old.destination_topic_id;
    copy.partition_count = old.partition_count;
    copy.replication_factor = old.replication_factor;
    copy.topic_configs.reserve(old.topic_configs.size());
    for (const auto& [key, value] : old.topic_configs) {
        copy.topic_configs.emplace(key, value);
    }
    copy.start_offset_ts = old.start_offset_ts;

    return copy;
}

topic_metadata_mirroring_config copy_topic_metadata_mirroring_config(
  const topic_metadata_mirroring_config& old) {
    topic_metadata_mirroring_config copy;

    copy.is_enabled = old.is_enabled;
    copy.task_interval = old.task_interval;
    copy.topic_name_filters = old.topic_name_filters.copy();
    copy.topic_properties_to_mirror = old.topic_properties_to_mirror;
    copy.exclude_default = old.exclude_default;
    copy.starting_offset = old.starting_offset;

    return copy;
}

consumer_groups_mirroring_config copy_consumer_groups_mirroring_config(
  const consumer_groups_mirroring_config& old) {
    consumer_groups_mirroring_config copy;

    copy.is_enabled = old.is_enabled;
    copy.task_interval = old.task_interval;
    copy.filters = old.filters.copy();
    return copy;
}

security_settings_sync_config
copy_security_settings_sync_config(const security_settings_sync_config& old) {
    security_settings_sync_config copy;

    copy.is_enabled = old.is_enabled;
    copy.task_interval = old.task_interval;
    copy.acl_filters = old.acl_filters.copy();
    return copy;
}

} // namespace

ss::future<link_state> copy_link_state(const link_state& old) {
    ssx::async_counter cnt;
    link_state copy;
    copy.status = old.status;
    copy.mirror_topics.reserve(old.mirror_topics.size());

    co_await ssx::async_for_each_counter(
      cnt, old.mirror_topics, [&](const auto& pair) {
          copy.mirror_topics.emplace(
            pair.first, copy_mirror_topic_metadata(pair.second));
      });

    co_return copy;
}

link_configuration copy_link_configuration(const link_configuration& old) {
    link_configuration copy;
    copy.topic_metadata_mirroring_cfg = copy_topic_metadata_mirroring_config(
      old.topic_metadata_mirroring_cfg);
    copy.consumer_groups_mirroring_cfg = copy_consumer_groups_mirroring_config(
      old.consumer_groups_mirroring_cfg);
    copy.security_settings_sync_cfg = copy_security_settings_sync_config(
      old.security_settings_sync_cfg);
    copy.schema_registry_sync_cfg = old.schema_registry_sync_cfg;

    return copy;
}

ss::future<chunked_hash_map<id_t, metadata>>
copy_links_for_snapshot(chunked_hash_map<id_t, metadata_ptr> links) {
    chunked_hash_map<id_t, metadata> copy;
    copy.reserve(links.size());
    for (const auto& [id, md_ptr] : links) {
        metadata md;
        md.name = md_ptr->name;
        md.uuid = md_ptr->uuid;
        md.connection = md_ptr->connection;
        md.state = co_await copy_link_state(md_ptr->state);
        md.configuration = copy_link_configuration(md_ptr->configuration);
        copy.emplace(id, std::move(md));
    }
    co_return copy;
}
ss::future<chunked_hash_map<id_t, metadata_ptr>>
copy_links_from_snapshot(const chunked_hash_map<id_t, metadata>& links) {
    chunked_hash_map<id_t, metadata_ptr> copy;
    copy.reserve(links.size());
    for (const auto& [id, md] : links) {
        auto metadata_copy = co_await copy_metadata(&md);
        copy.emplace(id, std::move(metadata_copy));
    }

    co_return copy;
}
ss::future<ss::lw_shared_ptr<metadata>> copy_metadata(const metadata* old) {
    auto copy = ss::make_lw_shared<metadata>();
    copy->name = old->name;
    copy->uuid = old->uuid;
    copy->connection = old->connection;
    copy->state = co_await copy_link_state(old->state);
    copy->configuration = copy_link_configuration(old->configuration);

    co_return copy;
}
} // namespace cluster::cluster_link
