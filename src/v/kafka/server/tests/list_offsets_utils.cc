/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kafka/server/tests/list_offsets_utils.h"

#include "kafka/client/transport.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/schemata/list_offset_request.h"
#include "kafka/protocol/schemata/list_offset_response.h"

namespace tests {

ss::future<kafka_list_offsets_transport::pid_to_offset_map_t>
kafka_list_offsets_transport::list_offsets(
  model::topic topic_name, pid_to_timestamp_map_t ts_per_partition) {
    kafka::list_offsets_request req;

    req.data.topics.emplace_back(kafka::list_offset_topic{
      .name = std::move(topic_name),
    });
    for (const auto& [pid, ts] : ts_per_partition) {
        req.data.topics[0].partitions.emplace_back(kafka::list_offset_partition{
          .partition_index = pid, .timestamp = ts});
    }
    auto resp = co_await _transport.dispatch(std::move(req));
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Expected 1 topic, got {}", resp.data.topics.size()));
    }
    pid_to_offset_map_t ret;
    for (const auto& p_res : resp.data.topics[0].partitions) {
        if (p_res.error_code != kafka::error_code::none) {
            throw std::runtime_error(fmt::format(
              "Error for partition {}: {}",
              p_res.partition_index,
              p_res.error_code));
        }
        ret[p_res.partition_index] = p_res.offset;
    }
    co_return ret;
}

ss::future<model::offset>
kafka_list_offsets_transport::start_offset_for_partition(
  model::topic topic_name, model::partition_id pid) {
    return list_offset_for_partition(
      std::move(topic_name),
      pid,
      kafka::list_offsets_request::earliest_timestamp);
}

ss::future<model::offset>
kafka_list_offsets_transport::high_watermark_for_partition(
  model::topic topic_name, model::partition_id pid) {
    return list_offset_for_partition(
      std::move(topic_name),
      pid,
      kafka::list_offsets_request::latest_timestamp);
}

} // namespace tests
