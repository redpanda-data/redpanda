#pragma once
#include "cluster/types.h"

cluster::partition_assignment create_test_assignment(
  const sstring& topic,
  int partition_id,
  std::vector<std::pair<uint32_t, uint32_t>> shards_assignment,
  int group_id) {
    cluster::partition_assignment p_as{
      .group = raft::group_id(group_id),
      .ntp = model::ntp{
        .ns = model::ns("test"),
        .tp = {.topic = model::topic(topic),
               .partition = model::partition_id(partition_id)}}};
    std::transform(
      shards_assignment.begin(),
      shards_assignment.end(),
      std::back_inserter(p_as.replicas),
      [](const std::pair<uint32_t, uint32_t>& node_shard) {
          return cluster::broker_shard{
            .node_id = model::node_id(node_shard.first),
            .shard = node_shard.second,
          };
      });
    return p_as;
}
