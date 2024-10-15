/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/scheduling/leader_balancer_constraints.h"

#include "base/vassert.h"
#include "model/metadata.h"

namespace cluster::leader_balancer_types {

even_topic_distribution_constraint::even_topic_distribution_constraint(
  const group_id_to_topic_id& group_to_topic_id,
  const shard_index& si,
  const muted_index& mi)
  : _si(si)
  , _mi(mi)
  , _group_to_topic_id(group_to_topic_id) {
    rebuild_indexes();
    calc_topic_skew();
    calculate_error();
}

void even_topic_distribution_constraint::update_index(const reassignment& r) {
    // _topic_partition_index, _topic_replica_index, and _topic_opt_leaders
    // will not change from moving leadership. They will only change if the
    // replicas themselves are moved. Hence no need to update them here.

    auto topic_id = group_to_topic_id().at(r.group);

    // Update _topic_skew and _error

    auto& skew = _topic_skew.at(topic_id);
    _error -= skew;
    skew = adjusted_error(skew, topic_id, r.from, r.to);
    _error += skew;

    // Update _topic_shard_index
    _topic_shard_index.at(topic_id).at(r.from) -= 1;
    _topic_shard_index.at(topic_id).at(r.to) += 1;
}

void even_topic_distribution_constraint::rebuild_indexes() {
    _topic_shard_index.clear();
    _topic_replica_index.clear();
    _topic_partition_index.clear();

    for (const auto& broker_shard : si().shards()) {
        for (const auto& group_p : broker_shard.second) {
            auto topic_id = group_to_topic_id().at(group_p.first);

            _topic_shard_index[topic_id][broker_shard.first] += 1;
            _topic_partition_index[topic_id] += 1;

            // Some of the replicas may not have leadership. So add
            // all replica shards here.
            for (const auto& replica_bs : group_p.second) {
                _topic_replica_index[topic_id].insert(replica_bs);
                _topic_shard_index[topic_id].try_emplace(replica_bs);
            }
        }
    }
}

/**
 *  Used to calculate the initial values for the error this constraint
 *  is trying to minimize. The goal here is to calculate a per topic
 *  error(or skew) where the error is zero if the leaders of a topic's
 *  partitions are evenly distributed on every shard. And where the error
 *  grows to +infinity the more skewed the leadership assignment is to a
 *  subset of shards. The equations used can be summarized as;
 *
 *  skew[topic_i] = SUM(leaders[shard_i, topic_i] - opt[topic_i])^2
 *  opt[topic_i]  = total_partitions[topic_i] / total_shards[topic_i]
 *  where total_shards is the number of shards a topic has replicas on.
 *        total_partitions is the number of partitions the topic has.
 */
void even_topic_distribution_constraint::calc_topic_skew() {
    _topic_skew.clear();
    _topic_opt_leaders.clear();

    for (const auto& topic : _topic_shard_index) {
        auto topic_partitions = static_cast<double>(
          _topic_partition_index.at(topic.first));
        auto topic_replicas = static_cast<double>(
          _topic_replica_index.at(topic.first).size());
        auto opt_leaders = topic_partitions
                           / std::min(topic_replicas, topic_partitions);
        _topic_opt_leaders[topic.first] = opt_leaders;

        auto& skew = _topic_skew[topic.first];
        skew = 0;

        for (const auto& shard : topic.second) {
            auto leaders = static_cast<double>(shard.second);

            skew += pow(leaders - opt_leaders, 2);
        }
    }
}

/*
 * Compute new error for the given reassignment.
 */
double even_topic_distribution_constraint::adjusted_error(
  double current_error,
  const topic_id_t& topic_id,
  const model::broker_shard& from,
  const model::broker_shard& to) const {
    auto opt_leaders = _topic_opt_leaders.at(topic_id);

    const auto& topic_leaders = _topic_shard_index.at(topic_id);

    double from_shard_leaders = 0;
    const auto from_it = topic_leaders.find(from);
    if (from_it != topic_leaders.cend()) {
        from_shard_leaders = static_cast<double>(from_it->second);
    } else {
        // If there are no leaders for the topic on the from shard
        // then there is nothing to move and no change to the error.
        return current_error;
    }

    double to_shard_leaders = 0;
    const auto to_it = topic_leaders.find(to);
    if (to_it != topic_leaders.cend()) {
        to_shard_leaders = static_cast<double>(to_it->second);
    }

    // Subtract old weights
    current_error -= pow(from_shard_leaders - opt_leaders, 2);
    current_error -= pow(to_shard_leaders - opt_leaders, 2);

    // Add new weights
    current_error += pow((from_shard_leaders - 1) - opt_leaders, 2);
    current_error += pow((to_shard_leaders + 1) - opt_leaders, 2);

    return current_error;
}

double even_shard_load_constraint::adjusted_error(
  double current_error,
  const model::broker_shard& from,
  const model::broker_shard& to) const {
    auto target_load = calc_target_load();
    const auto from_count = static_cast<double>(si().shards().at(from).size());
    const auto to_count = static_cast<double>(si().shards().at(to).size());
    // subtract original contribution
    current_error -= pow(from_count - target_load, 2);
    current_error -= pow(to_count - target_load, 2);
    // add back in the adjusted amount
    current_error += pow(from_count - 1 - target_load, 2);
    current_error += pow(to_count + 1 - target_load, 2);
    return current_error;
}

size_t even_shard_load_constraint::num_groups() const {
    return std::accumulate(
      si().shards().cbegin(),
      si().shards().cend(),
      size_t{0},
      [this](auto acc, const auto& e) {
          /*
           * we aren't going to attempt to move leadership between muted
           * nodes, so we only count groups with leaders on non-muted nodes
           * when calculating error / target load.
           */
          if (mi().muted_nodes().contains(e.first.node_id)) {
              return acc;
          }
          return acc + e.second.size();
      });
}

double even_shard_load_constraint::calc_target_load() const {
    return static_cast<double>(_num_groups) / static_cast<double>(_num_cores);
}

size_t even_shard_load_constraint::num_cores() const {
    return std::count_if(
      si().shards().cbegin(), si().shards().cend(), [this](const auto& core) {
          return !mi().muted_nodes().contains(core.first.node_id);
      });
}

std::pair<
  even_shard_load_constraint::load_t,
  even_shard_load_constraint::load_map_t>
even_shard_load_constraint::build_load_indexes() const {
    load_t load;
    load_map_t load_map;

    load.reserve(si().shards().size());
    load_map.reserve(si().shards().size());
    for (auto it = si().shards().cbegin(); it != si().shards().cend(); ++it) {
        load.push_back(it);
        load_map.emplace(it->first, it->second.size());
    }
    std::sort(load.begin(), load.end(), [](const auto& a, const auto& b) {
        return a->second.size() < b->second.size();
    });

    return {load, load_map};
}

std::vector<shard_load> even_shard_load_constraint::stats() const {
    auto [load, _] = build_load_indexes();
    std::vector<shard_load> ret;
    ret.reserve(load.size());
    std::transform(
      load.cbegin(), load.cend(), std::back_inserter(ret), [](const auto& e) {
          // oddly, absl::btree::size returns a signed type
          return shard_load{e->first, static_cast<size_t>(e->second.size())};
      });
    return ret;
}

double pinning_constraint::evaluate_internal(const reassignment& r) {
    int diff = 0;

    const leaders_preference* preference = &_preference_idx.default_preference;

    topic_id_t topic_id = _group2topic.get().at(r.group);
    auto pref_it = _preference_idx.topic2preference.find(topic_id);
    if (pref_it != _preference_idx.topic2preference.end()) {
        preference = &pref_it->second;
    }

    if (preference->racks.empty()) {
        return diff;
    }

    auto from_it = _preference_idx.node2rack.find(r.from.node_id);
    if (
      from_it != _preference_idx.node2rack.end()
      && preference->racks.contains(from_it->second)) {
        diff -= 1;
    }

    auto to_it = _preference_idx.node2rack.find(r.to.node_id);
    if (
      to_it != _preference_idx.node2rack.end()
      && preference->racks.contains(to_it->second)) {
        diff += 1;
    }

    return diff;
}

} // namespace cluster::leader_balancer_types
