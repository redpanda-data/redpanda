// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/nextgen/coordinator.h"

#include "config/configuration.h"
#include "kafka/protocol/logger.h"
#include "metrics/prometheus_sanitize.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>

namespace kafka::nextgen {

ss::future<> coordinator::start() {
    setup_metrics();
    co_return;
}

void coordinator::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka_nextgen"),
      {
        sm::make_counter(
          "heartbeat_total",
          [this] { return _heartbeat_total; },
          sm::description(
            "Total number of KIP-848 consumer group heartbeats processed")),
        sm::make_counter(
          "group_state_transitions_total",
          [this] { return _state_transitions_total; },
          sm::description(
            "Total number of KIP-848 consumer group member state transitions")),
      },
      {},
      {sm::shard_label});
}

ss::future<heartbeat_result> coordinator::heartbeat(
  ss::sstring group_id, ss::sstring member_id, int32_t member_epoch) {
    ++_heartbeat_total;
    auto& group = _groups[group_id];

    if (member_epoch == -1) {
        ss::sstring new_id = uuid_t::create();
        group.members[new_id] = member_info{
          .epoch = 0, .state = member_state::reconciling};
        ++_state_transitions_total;
        vlog(
          klog.info,
          "kip848: group {} member {} -> reconciling (epoch=0)",
          group_id,
          new_id);
        co_return heartbeat_result{
          .ec = error_code::none,
          .member_id = std::move(new_id),
          .member_epoch = 0};
    }

    auto it = group.members.find(member_id);
    if (it == group.members.end()) {
        co_return heartbeat_result{.ec = error_code::unknown_member_id};
    }

    auto& m = it->second;
    if (m.epoch != member_epoch) {
        co_return heartbeat_result{.ec = error_code::fenced_member_epoch};
    }

    m.epoch += 1;
    auto prev = m.state;
    if (m.state == member_state::reconciling) {
        m.state = member_state::stable;
        ++_state_transitions_total;
    }
    vlog(
      klog.info,
      "kip848: group {} member {} {} -> {} (epoch={})",
      group_id,
      member_id,
      prev == member_state::reconciling ? "reconciling" : "stable",
      m.state == member_state::stable ? "stable" : "reconciling",
      m.epoch);
    co_return heartbeat_result{
      .ec = error_code::none, .member_id = member_id, .member_epoch = m.epoch};
}

ss::future<> coordinator::stop() {
    _groups.clear();
    co_return;
}

} // namespace kafka::nextgen
