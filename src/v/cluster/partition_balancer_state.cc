/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_state.h"

#include "cluster/logger.h"
#include "cluster/scheduling/partition_allocator.h"

#include <absl/container/flat_hash_set.h>

namespace cluster {

partition_balancer_state::partition_balancer_state(
  ss::sharded<topic_table>& topic_table,
  ss::sharded<members_table>& members_table,
  ss::sharded<partition_allocator>& pa)
  : _topic_table(topic_table.local())
  , _members_table(members_table.local())
  , _partition_allocator(pa.local()) {}

void partition_balancer_state::handle_ntp_update(
  const model::ns& ns,
  const model::topic& tp,
  model::partition_id p_id,
  const std::vector<model::broker_shard>& prev,
  const std::vector<model::broker_shard>& next) {
    if (_partition_allocator.is_rack_awareness_enabled()) {
        absl::flat_hash_set<model::rack_id> racks;
        bool is_rack_constraint_violated = false;
        for (const auto& bs : next) {
            auto rack = _partition_allocator.state().get_rack_id(bs.node_id);
            if (rack) {
                auto res = racks.insert(std::move(*rack));
                if (!res.second) {
                    is_rack_constraint_violated = true;
                    break;
                }
            }
        }

        model::ntp ntp(ns, tp, p_id);
        if (is_rack_constraint_violated) {
            auto res = _ntps_with_broken_rack_constraint.insert(ntp);
            if (res.second) {
                vlog(
                  clusterlog.debug,
                  "rack constraint violated for ntp: {}, "
                  "replica set change: {} -> {}",
                  ntp,
                  prev,
                  next);
            }
        } else {
            auto erased = _ntps_with_broken_rack_constraint.erase(ntp);
            if (erased > 0) {
                vlog(
                  clusterlog.debug,
                  "rack constraint restored for ntp: {}, "
                  "replica set change: {} -> {}",
                  ntp,
                  prev,
                  next);
            }
        }
    }
}

} // namespace cluster
