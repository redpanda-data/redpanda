/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster_link/model/types.h"
#include "model/fundamental.h"
#include "model/record.h"

namespace cluster::cluster_link::testing {
model::record_batch
  create_upsert_command(model::offset, ::cluster_link::model::metadata);
model::record_batch
create_remove_command(::cluster_link::model::name_t, bool force);
model::record_batch create_add_mirror_topic_command(
  ::cluster_link::model::id_t, ::cluster_link::model::add_mirror_topic_cmd);
model::record_batch create_delete_mirror_topic_command(
  ::cluster_link::model::id_t, ::cluster_link::model::delete_mirror_topic_cmd);
model::record_batch create_update_mirror_topic_status_command(
  ::cluster_link::model::id_t,
  ::cluster_link::model::update_mirror_topic_status_cmd);
model::record_batch create_batch_update_mirror_topic_status_command(
  ::cluster_link::model::id_t,
  ::cluster_link::model::batch_update_mirror_topic_status_cmd);
model::record_batch create_update_mirror_topic_properties_command(
  ::cluster_link::model::id_t,
  ::cluster_link::model::update_mirror_topic_properties_cmd);
model::record_batch create_update_cluster_link_configuration_command(
  ::cluster_link::model::id_t,
  ::cluster_link::model::update_cluster_link_configuration_cmd);

::cluster_link::model::mirror_topic_metadata create_mirror_topic_metadata(
  ::cluster_link::model::mirror_topic_status,
  ::model::topic source_topic_name,
  std::optional<::model::topic_id> source_topic_id = std::nullopt,
  std::optional<::model::topic_id> destination_topoic_id = std::nullopt,
  chunked_hash_map<ss::sstring, ss::sstring> topic_configs = {},
  int32_t partition_count = 3,
  std::optional<int16_t> replication_factor = std::nullopt);

template<typename... Args>
void set_link_mirror_topics(
  ::cluster_link::model::metadata& link,
  const ::model::topic& topic,
  Args&&... args) {
    auto mirror_topic_metadata = testing::create_mirror_topic_metadata(
      std::forward<Args>(args)...);

    ::cluster_link::model::link_state::mirror_topics_t mirror_topics;
    mirror_topics.emplace(topic, std::move(mirror_topic_metadata));
    link.state.set_mirror_topics(std::move(mirror_topics));
}
} // namespace cluster::cluster_link::testing
