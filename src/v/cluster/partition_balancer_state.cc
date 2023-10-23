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

#include "cluster/cluster_utils.h"
#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <absl/container/flat_hash_set.h>

namespace cluster {

partition_balancer_state::partition_balancer_state(
  ss::sharded<topic_table>& topic_table,
  ss::sharded<members_table>& members_table,
  ss::sharded<partition_allocator>& pa,
  ss::sharded<node_status_table>& nst)
  : _topic_table(topic_table.local())
  , _members_table(members_table.local())
  , _partition_allocator(pa.local())
  , _node_status(nst.local())
  , _probe(*this) {}

void partition_balancer_state::handle_ntp_move_begin_or_cancel(
  const model::ns& ns,
  const model::topic& tp,
  model::partition_id p_id,
  const std::vector<model::broker_shard>& prev,
  const std::vector<model::broker_shard>& next) {
    if (_partition_allocator.is_rack_awareness_enabled()) {
        absl::flat_hash_set<model::rack_id> racks;
        bool is_rack_constraint_violated = false;
        for (const auto& bs : next) {
            auto rack = _members_table.get_node_rack_id(bs.node_id);
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
            _ntps_with_broken_rack_constraint_revision++;
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
            _ntps_with_broken_rack_constraint_revision++;
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

void partition_balancer_state::handle_ntp_move_finish(
  const model::ntp& ntp, const std::vector<model::broker_shard>& replicas) {
    auto it = _ntps_to_force_reconfigure.find(ntp);
    if (it == _ntps_to_force_reconfigure.end()) {
        return;
    }
    auto& entries = it->second;
    auto num_erased = std::erase_if(entries, [&replicas](const auto& entry) {
        // check if the new replica set contains any defunct nodes that were
        // intended to be removed.
        bool has_defunct_nodes = std::any_of(
          entry.defunct_nodes.begin(),
          entry.defunct_nodes.end(),
          [&replicas](const model::node_id& id) {
              return contains_node(replicas, id);
          });
        return !has_defunct_nodes;
    });
    if (num_erased > 0) {
        _ntps_to_force_reconfigure_revision++;
    }
    if (entries.size() == 0) {
        _ntps_to_force_reconfigure.erase(it);
        _ntps_to_force_reconfigure_revision++;
    }
}

void partition_balancer_state::handle_ntp_delete(const model::ntp& ntp) {
    auto it = _ntps_to_force_reconfigure.find(ntp);
    if (it == _ntps_to_force_reconfigure.end()) {
        return;
    }
    auto topic_md = _topic_table.get_topic_metadata_ref({ntp.ns, ntp.tp.topic});
    if (!topic_md) {
        // topic no longer exists
        _ntps_to_force_reconfigure.erase(it);
        _ntps_to_force_reconfigure_revision++;
        vlog(
          clusterlog.debug,
          "marking force repair action as finished for partition: {} because "
          "the topic was deleted.",
          it->first);
        return;
    }
    // A newer version of the topic exists.
    // delete all entries for older revision of the topic.
    auto& entries = it->second;
    auto new_topic_revision = topic_md.value().get().get_revision();
    auto num_erased = std::erase_if(entries, [&](const auto& entry) {
        return entry.topic_revision < new_topic_revision;
    });
    if (num_erased > 0) {
        _ntps_to_force_reconfigure_revision++;
    }
    if (entries.size() == 0) {
        _ntps_to_force_reconfigure.erase(it);
        _ntps_to_force_reconfigure_revision++;
        vlog(
          clusterlog.debug,
          "marking force repair action as finished for partition: {} because "
          "the topic was deleted.",
          it->first);
    }
}

ss::future<>
partition_balancer_state::apply_snapshot(const controller_snapshot& snap) {
    if (!_partition_allocator.is_rack_awareness_enabled()) {
        co_return;
    }

    absl::flat_hash_map<model::node_id, model::rack_id> node2rack;
    for (const auto& [id, node] : snap.members.nodes) {
        if (node.broker.rack()) {
            node2rack[id] = *node.broker.rack();
        }
    }

    auto is_rack_placement_valid =
      [&](const std::vector<model::broker_shard>& replicas) {
          absl::flat_hash_set<model::rack_id> racks;
          for (auto [node_id, shard] : replicas) {
              auto it = node2rack.find(node_id);
              if (it != node2rack.end() && !racks.insert(it->second).second) {
                  return false;
              }
          }
          return true;
      };

    _ntps_with_broken_rack_constraint.clear();
    _ntps_with_broken_rack_constraint_revision++;
    for (const auto& [ns_tp, topic] : snap.topics.topics) {
        for (const auto& [p_id, partition] : topic.partitions) {
            const std::vector<model::broker_shard>* replicas
              = &partition.replicas;

            if (auto it = topic.updates.find(p_id); it != topic.updates.end()) {
                const auto& update = it->second;
                if (!is_cancelled_state(update.state)) {
                    replicas = &update.target_assignment;
                }
            }

            if (!is_rack_placement_valid(*replicas)) {
                _ntps_with_broken_rack_constraint.emplace(
                  ns_tp.ns, ns_tp.tp, p_id);
                _ntps_with_broken_rack_constraint_revision++;
            }

            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return;
}

partition_balancer_state::probe::probe(const partition_balancer_state& parent)
  : _parent(parent) {
    if (
      config::shard_local_cfg().disable_metrics() || ss::this_shard_id() != 0) {
        return;
    }

    setup_metrics(_metrics);
    setup_metrics(_public_metrics);
}

void partition_balancer_state::probe::setup_metrics(
  metrics::metric_groups_base& metrics) {
    namespace sm = ss::metrics;
    metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:partition"),
      {
        sm::make_gauge(
          "num_with_broken_rack_constraint",
          [this] { return _parent.ntps_with_broken_rack_constraint().size(); },
          sm::description("Number of partitions that don't satisfy the rack "
                          "awareness constraint"))
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
