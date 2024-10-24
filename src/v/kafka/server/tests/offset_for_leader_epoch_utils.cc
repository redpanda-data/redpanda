/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kafka/server/tests/offset_for_leader_epoch_utils.h"

#include "base/vlog.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_for_leader_epoch.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_request.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_response.h"
#include "kafka/protocol/types.h"

#include <seastar/util/log.hh>

namespace tests {

ss::future<kafka_offset_for_epoch_transport::pid_to_offset_map_t>
kafka_offset_for_epoch_transport::offsets_for_leaders(
  model::topic topic_name, pid_to_term_map_t term_per_partition) {
    kafka::offset_for_leader_epoch_request req;
    kafka::offset_for_leader_topic t{topic_name, {}, {}};
    for (const auto [pid, term] : term_per_partition) {
        t.partitions.emplace_back(kafka::offset_for_leader_partition{
          .partition = pid,
          .current_leader_epoch = kafka::leader_epoch{-1},
          .leader_epoch = kafka::leader_epoch(term()),
        });
    }
    req.data.topics.emplace_back(std::move(t));
    auto resp = co_await _transport.dispatch(std::move(req));
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Expected 1 topic, got {}", resp.data.topics.size()));
    }
    pid_to_offset_map_t ret;
    for (const auto& p_res : resp.data.topics[0].partitions) {
        // NOTE: offset_out_of_range returns with a low watermark of -1
        if (p_res.error_code == kafka::error_code::none) {
            ret.emplace(p_res.partition, p_res.end_offset);
            continue;
        }
        throw std::runtime_error(fmt::format(
          "Error for partition {}: {}", p_res.partition, p_res.error_code));
    }
    co_return ret;
}

} // namespace tests
