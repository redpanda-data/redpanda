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

#include "model/metadata.h"
#include "vassert.h"

namespace cluster::leader_balancer_types {

even_topic_distributon_constraint::even_topic_distributon_constraint(
  group_id_to_topic_revision_t group_to_topic_rev,
  shard_index si,
  const muted_index& mi)
  : _si(std::move(si))
  , _mi(mi)
  , _group_to_topic_rev(std::move(group_to_topic_rev)) {
    rebuild_indexes();
    calc_topic_skew();
    calculate_error();
}

void even_topic_distributon_constraint::update_index(const reassignment& r) {
    // _topic_partition_index, _topic_replica_index, and _topic_opt_leaders
    // will not change from moving leadership. They will only change if the
    // replicas themselves are moved. Hence no need to update them here.

    auto topic_id = group_to_topic_id().at(r.group);

    // Update _topic_skew and _error

    auto& skew = _topic_skew.at(topic_id);
    _error -= skew;
    skew = adjusted_error(skew, topic_id, r.from, r.to);
    _error += skew;

    // Update _topic_node_index

    auto& groups_from_node = _topic_node_index.at(topic_id).at(r.from.node_id);
    auto it = std::find_if(
      groups_from_node.begin(),
      groups_from_node.end(),
      [&r](const auto& g_info) { return r.group == g_info.group_id; });

    vassert(it != groups_from_node.end(), "reassigning non-existent group");

    auto moved_group_info = std::move(*it);
    groups_from_node.erase(it);

    _topic_node_index[topic_id][r.to.node_id].emplace_back(
      moved_group_info.group_id, r.to, std::move(moved_group_info.replicas));

    // Update _si

    _si.update_index(r);
}

std::optional<reassignment>
even_topic_distributon_constraint::recommended_reassignment() {
    // Sort topics based on topic error here
    std::vector<decltype(_topic_node_index)::const_iterator> sorted_topics;
    sorted_topics.reserve(_topic_node_index.size());

    for (auto it = _topic_node_index.cbegin(); it != _topic_node_index.cend();
         ++it) {
        sorted_topics.push_back(it);
    }

    std::sort(
      sorted_topics.begin(),
      sorted_topics.end(),
      [this](const auto& a, const auto& b) {
          return _topic_skew[a->first] > _topic_skew[b->first];
      });

    // Look for a topic with the most skew
    for (const auto& topic : sorted_topics) {
        const auto& nodes = topic->second;

        if (nodes.size() == 0) {
            continue;
        }

        std::vector<decltype(nodes.cbegin())> nodes_sorted;
        nodes_sorted.reserve(nodes.size());

        for (auto it = nodes.cbegin(); it != nodes.cend(); ++it) {
            nodes_sorted.push_back(it);
        }

        std::sort(
          nodes_sorted.begin(),
          nodes_sorted.end(),
          [](const auto& a, const auto& b) {
              return a->second.size() > b->second.size();
          });

        // Try to move leadership off the node with the most leadership.
        for (const auto& node : nodes_sorted) {
            // Don't try moving a group from a muted node.
            if (mi().muted_nodes().contains(node->first)) {
                continue;
            }

            for (const auto& g_info : node->second) {
                const auto& leader = g_info.leader;
                const auto& group = g_info.group_id;
                const auto& replicas = g_info.replicas;

                // Don't try moving any groups that are currently muted.
                if (mi().muted_groups().contains(group)) {
                    continue;
                }

                for (const auto& replica : replicas) {
                    // Don't try a move to a different shard to on the same
                    // node. As it won't decrease error
                    if (replica.node_id == node->first || leader == replica) {
                        continue;
                    }

                    // Don't try moving group to a muted node.
                    if (mi().muted_nodes().contains(replica.node_id)) {
                        continue;
                    }

                    reassignment r{group, leader, replica};

                    if (evaluate_internal(r) > error_jitter) {
                        return r;
                    }
                }
            }
        }
    }

    return std::nullopt;
}

void even_topic_distributon_constraint::rebuild_indexes() {
    _topic_node_index.clear();
    _topic_replica_index.clear();
    _topic_partition_index.clear();

    for (const auto& broker_shard : si().shards()) {
        for (const auto& group_p : broker_shard.second) {
            auto topic_id = group_to_topic_id().at(group_p.first);
            const auto& node_id = broker_shard.first.node_id;

            _topic_node_index[topic_id][node_id].emplace_back(
              group_p.first, broker_shard.first, group_p.second);
            _topic_partition_index[topic_id] += 1;

            // Some of the replicas may not have leadership. So add
            // all replica nodes here.
            for (const auto& replica_bs : group_p.second) {
                _topic_replica_index[topic_id].insert(replica_bs.node_id);
                _topic_node_index[topic_id].try_emplace(replica_bs.node_id);
            }
        }
    }
}

/**
 *  Used to calculate the initial values for the error this constraint
 *  is trying to minimize. The goal here is to calculate a per topic
 *  error(or skew) where the error is zero if the leaders of a topic's
 *  partitions are evenly distributed on every node. And where the error
 *  grows to +infinity the more skewed the leadership assignment is to a
 *  subset of nodes. The equations used can be summarized as;
 *
 *  skew[topic_i] = SUM(leaders[node_i, topic_i] - opt[topic_i])^2
 *  opt[topic_i]  = total_partitions[topic_i] / total_nodes[topic_i]
 *  where total_nodes is the number of nodes a topic has replicas on.
 *        total_partitions is the number of partitions the topic has.
 */
void even_topic_distributon_constraint::calc_topic_skew() {
    _topic_skew.clear();
    _topic_opt_leaders.clear();

    for (const auto& topic : _topic_node_index) {
        auto topic_partitions = static_cast<double>(
          _topic_partition_index.at(topic.first));
        auto topic_replicas = static_cast<double>(
          _topic_replica_index.at(topic.first).size());
        auto opt_leaders = topic_partitions
                           / std::min(topic_replicas, topic_partitions);
        _topic_opt_leaders[topic.first] = opt_leaders;

        auto& skew = _topic_skew[topic.first];
        skew = 0;

        for (const auto& node : topic.second) {
            auto leaders = static_cast<double>(node.second.size());

            skew += pow(leaders - opt_leaders, 2);
        }
    }
}

/*
 * Compute new error for the given reassignment.
 */
double even_topic_distributon_constraint::adjusted_error(
  double current_error,
  const topic_id_t& topic_id,
  const model::broker_shard& from,
  const model::broker_shard& to) const {
    // Moving leadership between shards on the same node doesn't change
    // error for this constraint.
    if (from.node_id == to.node_id) {
        return current_error;
    }

    auto opt_leaders = _topic_opt_leaders.at(topic_id);

    const auto& topic_leaders = _topic_node_index.at(topic_id);

    double from_node_leaders = 0;
    const auto from_it = topic_leaders.find(from.node_id);
    if (from_it != topic_leaders.cend()) {
        from_node_leaders = static_cast<double>(from_it->second.size());
    } else {
        // If there are no leaders for the topic on the from node
        // then there is nothing to move and no change to the error.
        return current_error;
    }

    double to_node_leaders = 0;
    const auto to_it = topic_leaders.find(to.node_id);
    if (to_it != topic_leaders.cend()) {
        to_node_leaders = static_cast<double>(to_it->second.size());
    }

    // Subtract old weights
    current_error -= pow(from_node_leaders - opt_leaders, 2);
    current_error -= pow(to_node_leaders - opt_leaders, 2);

    // Add new weights
    current_error += pow((from_node_leaders - 1) - opt_leaders, 2);
    current_error += pow((to_node_leaders + 1) - opt_leaders, 2);

    return current_error;
}

std::optional<reassignment>
even_shard_load_constraint::recommended_reassignment() {
    auto [load, load_map] = build_load_indexes();
    const auto curr_error = error();

    // Consider each group from high load core, and record the reassignment
    // involving the lowest load "to" core.
    for (const auto& from : boost::adaptors::reverse(load)) {
        if (mi().muted_nodes().contains(from->first.node_id)) {
            continue;
        }

        constexpr size_t load_unset = std::numeric_limits<size_t>::max();
        size_t lowest_load = load_unset;
        reassignment lowest_reassign{};

        // Consider each group from high load core, and record the
        // reassignment involving the lowest load "to" core.
        for (const auto& group : from->second) {
            if (mi().muted_groups().contains(group.first)) {
                continue;
            }

            // iterate over all the replicas and look for the lowest load
            // shard in the replica list
            for (const auto& to_shard : group.second) {
                auto load = load_map.at(to_shard);
                if (likely(load >= lowest_load)) {
                    // there is no point in evaluating this move, it is
                    // worse than the best one we've found so far.
                    continue;
                }

                if (mi().muted_nodes().contains(to_shard.node_id)) {
                    continue;
                }

                lowest_load = load;
                lowest_reassign = {group.first, from->first, to_shard};
            }
        }

        if (lowest_load != load_unset) {
            // We found a possible reassignment while looking at the current
            // "from" shard, and while it is the best possible reassignment
            // found it may not improve the error
            auto new_error = adjusted_error(
              curr_error, lowest_reassign.from, lowest_reassign.to);
            if (new_error + error_jitter < curr_error) {
                return lowest_reassign;
            }
        }
    }

    return std::nullopt;
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

} // namespace cluster::leader_balancer_types
