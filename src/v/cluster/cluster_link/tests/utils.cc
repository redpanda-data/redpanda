/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/tests/utils.h"

#include "cluster/commands.h"
#include "cluster_link/model/types.h"

namespace cluster::cluster_link::testing {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::batch_update_mirror_topic_status_cmd;
using ::cluster_link::model::delete_mirror_topic_cmd;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::mirror_topic_metadata;
using ::cluster_link::model::mirror_topic_status;
using ::cluster_link::model::name_t;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_status_cmd;

model::record_batch create_upsert_command(model::offset offset, metadata link) {
    cluster::cluster_link_upsert_cmd cmd(0, std::move(link));
    auto batch = cluster::serde_serialize_cmd(std::move(cmd));
    batch.header().base_offset = offset;
    return batch;
}

model::record_batch create_remove_command(name_t name, bool force) {
    cluster::cluster_link_remove_cmd cmd(
      0, {.link_name = std::move(name), .force = force});
    return cluster::serde_serialize_cmd(std::move(cmd));
}

model::record_batch
create_add_mirror_topic_command(id_t id, add_mirror_topic_cmd cmd) {
    cluster::cluster_link_add_mirror_topic_cmd add_cmd(id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(add_cmd));
}

model::record_batch
create_delete_mirror_topic_command(id_t id, delete_mirror_topic_cmd cmd) {
    cluster::cluster_link_delete_mirror_topic_cmd del_cmd(id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(del_cmd));
}

model::record_batch create_update_mirror_topic_status_command(
  id_t id, update_mirror_topic_status_cmd cmd) {
    cluster::cluster_link_update_mirror_topic_status_cmd update_cmd(
      id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(update_cmd));
}

model::record_batch create_batch_update_mirror_topic_status_command(
  id_t id, batch_update_mirror_topic_status_cmd cmd) {
    cluster::cluster_link_batch_update_mirror_topic_status_cmd update_cmd(
      id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(update_cmd));
}

model::record_batch create_update_mirror_topic_properties_command(
  id_t id, ::cluster_link::model::update_mirror_topic_properties_cmd cmd) {
    cluster::cluster_link_update_mirror_topic_properties_cmd update_cmd(
      id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(update_cmd));
}

model::record_batch create_update_cluster_link_configuration_command(
  id_t id, update_cluster_link_configuration_cmd cmd) {
    cluster::cluster_link_update_cluster_link_configuration_cmd update_cmd(
      id, std::move(cmd));
    return cluster::serde_serialize_cmd(std::move(update_cmd));
}

mirror_topic_metadata create_mirror_topic_metadata(
  mirror_topic_status status,
  ::model::topic source_topic_name,
  std::optional<::model::topic_id> source_topic_id,
  std::optional<::model::topic_id> destination_topic_id,
  chunked_hash_map<ss::sstring, ss::sstring> topic_configs,
  int32_t partition_count,
  std::optional<int16_t> replication_factor) {
    return {
      .status = status,
      .source_topic_id = source_topic_id,
      .source_topic_name = std::move(source_topic_name),
      .destination_topic_id = destination_topic_id.value_or(
        ::model::topic_id{uuid_t::create()}),
      .partition_count = partition_count,
      .replication_factor = replication_factor,
      .topic_configs = std::move(topic_configs),
    };
}
} // namespace cluster::cluster_link::testing
