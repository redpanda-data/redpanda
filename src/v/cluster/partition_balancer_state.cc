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

#include "absl/container/flat_hash_set.h"
#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/replicas_preference.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/coroutine/maybe_yield.hh>

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
  , _probe(*this) {
    if (ss::this_shard_id() == 0) {
        _topic_deltas_handle = _topic_table.register_topic_delta_notification(
          [this](const chunked_vector<topic_table_topic_delta>& deltas) {
              handle_topic_deltas(deltas);
          });
    }
}

ss::future<> partition_balancer_state::stop() {
    if (_topic_deltas_handle != cluster::notification_id_type_invalid) {
        _topic_table.unregister_topic_delta_notification(_topic_deltas_handle);
        _topic_deltas_handle = cluster::notification_id_type_invalid;
    }
    return ss::now();
}

bool partition_balancer_state::is_rack_awareness_enabled() const {
    return _partition_allocator.is_rack_awareness_enabled();
}

void partition_balancer_state::ensure_pinning_cache_seeded() {
    if (_pinning_cache_seeded) {
        return;
    }
    _topics_with_replica_pinning.clear();
    for (const auto& [tp_ns, md] : _topic_table.topics_map()) {
        const auto& pref
          = md.get_configuration().properties.replicas_preference;
        if (pref && pref->type != config::replicas_preference::type_t::none) {
            _topics_with_replica_pinning.insert(tp_ns);
        }
    }
    _pinning_cache_seeded = true;
}

void partition_balancer_state::handle_topic_deltas(
  const chunked_vector<topic_table_topic_delta>& deltas) {
    // Before first seed there is nothing to maintain; the next
    // ensure_pinning_cache_seeded() call reads live topic_table state.
    if (!_pinning_cache_seeded) {
        return;
    }

    using delta_type = topic_table_topic_delta_type;
    for (const auto& d : deltas) {
        switch (d.type) {
        case delta_type::added:
        case delta_type::properties_updated: {
            auto md_ref = _topic_table.get_topic_metadata_ref(d.ns_tp);
            if (!md_ref) {
                _topics_with_replica_pinning.erase(d.ns_tp);
                break;
            }
            const auto& pref = md_ref->get()
                                 .get_configuration()
                                 .properties.replicas_preference;
            if (
              pref && pref->type != config::replicas_preference::type_t::none) {
                _topics_with_replica_pinning.insert(d.ns_tp);
            } else {
                _topics_with_replica_pinning.erase(d.ns_tp);
            }
            break;
        }
        case delta_type::removed:
            _topics_with_replica_pinning.erase(d.ns_tp);
            break;
        }
    }
}

void partition_balancer_state::handle_ntp_move_begin_or_cancel(
  const model::ns& ns,
  const model::topic& tp,
  model::partition_id p_id,
  const std::vector<model::broker_shard>& prev,
  const std::vector<model::broker_shard>& next) {
    if (!_partition_allocator.is_rack_awareness_enabled()) {
        return;
    }

    model::ntp ntp(ns, tp, p_id);

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

void partition_balancer_state::handle_ntp_move_finish(
  const model::ntp& ntp, const std::vector<model::broker_shard>& replicas) {
    if (!_partition_allocator.is_rack_awareness_enabled()) {
        return;
    }

    absl::flat_hash_set<model::rack_id> racks;
    bool is_rack_constraint_violated = false;
    for (const auto& bs : replicas) {
        auto rack = _members_table.get_node_rack_id(bs.node_id);
        if (rack) {
            auto res = racks.insert(std::move(*rack));
            if (!res.second) {
                is_rack_constraint_violated = true;
                break;
            }
        }
    }

    if (is_rack_constraint_violated) {
        _ntps_with_broken_rack_constraint.insert(ntp);
    } else {
        _ntps_with_broken_rack_constraint.erase(ntp);
    }
    _ntps_with_broken_rack_constraint_revision++;
}

void partition_balancer_state::handle_ntp_delete(const model::ntp& ntp) {
    _ntps_with_broken_rack_constraint.erase(ntp);
    _ntps_with_broken_rack_constraint_revision++;
}

ss::future<>
partition_balancer_state::apply_snapshot(const controller_snapshot& snap) {
    bool rack_awareness = _partition_allocator.is_rack_awareness_enabled();

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

    // topic_table is being rebuilt from the snapshot, so any deltas the
    // pinning-cache notification callback may have observed are no longer
    // authoritative. Force a re-seed on the next read.
    reset_pinning_cache();

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

            if (rack_awareness && !is_rack_placement_valid(*replicas)) {
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
          sm::description(
            "Number of partitions that don't satisfy the rack "
            "awareness constraint"))
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
