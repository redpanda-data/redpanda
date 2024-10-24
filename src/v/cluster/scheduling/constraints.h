/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/members_table.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

class allocation_state;

inline constexpr std::string_view rack_label = "rack";

hard_constraint not_fully_allocated();
hard_constraint is_active();

hard_constraint on_node(model::node_id);

hard_constraint on_nodes(const std::vector<model::node_id>&);

hard_constraint distinct_from(const std::vector<model::broker_shard>&);

hard_constraint distinct_nodes();

/*
 * constraint checks that new partition won't violate max_disk_usage_ratio
 * partition_size is size of partition that is going to be allocated
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
hard_constraint disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

/*
 * Scores nodes based on partition count after all moves have been finished
 * returning `0` for fully allocated nodes and `max_capacity` for empty nodes.
 */
soft_constraint max_final_capacity();

/*
 * constraint scores nodes on free disk space
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
soft_constraint least_disk_filled(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

template<
  typename Mapper,
  typename LabelType =
    typename std::invoke_result_t<Mapper, model::node_id>::value_type>
concept LabelMapper = requires(Mapper mapper, model::node_id id) {
    { mapper(id) } -> std::convertible_to<std::optional<LabelType>>;
};

template<
  typename Mapper,
  typename T
  = typename std::invoke_result_t<Mapper, model::node_id>::value_type>
requires LabelMapper<Mapper, T>
soft_constraint
distinct_labels_preferred(const char* label_name, Mapper&& mapper) {
    class impl : public soft_constraint::impl {
    public:
        impl(const char* label_name, Mapper&& mapper)
          : _label_name(label_name)
          , _mapper(std::forward<Mapper>(mapper)) {}

        soft_constraint_evaluator make_evaluator(
          const allocated_partition& partition,
          std::optional<model::node_id> prev) const final {
            absl::flat_hash_map<T, size_t> frequency_map;
            std::optional<T> prev_label;

            for (auto& r : partition.replicas()) {
                const auto l = _mapper(r.node_id);
                if (!l) {
                    continue;
                }
                if (r.node_id == prev) {
                    prev_label = l;
                }
                frequency_map[*l] += 1;
            }

            return [this, frequency_map = std::move(frequency_map), prev_label](
                     const allocation_node& candidate_node) -> uint64_t {
                auto node_label = _mapper(candidate_node.id());
                if (!node_label) {
                    return (uint64_t)0;
                }

                auto it = frequency_map.find(*node_label);
                if (it == frequency_map.end()) {
                    return (uint64_t)soft_constraint::max_score;
                }

                auto count = it->second;
                if (node_label != prev_label) {
                    count += 1;
                }

                return (uint64_t)soft_constraint::max_score / count;
            };
        }

        ss::sstring name() const final {
            return fmt::format("distinct {} labels preferred", _label_name);
        }

        const char* _label_name;

        Mapper _mapper;
    };

    return soft_constraint(
      std::make_unique<impl>(label_name, std::forward<Mapper>(mapper)));
}

soft_constraint distinct_rack_preferred(const members_table&);

/// Scores nodes based on replica count queried from the supplied map (as
/// opposed to total node count). E.g. if the map represents topic partition
/// counts, this constraint will implement topic-aware replica placement.
///
/// NOTE: counts will be scaled by max node capacity.
soft_constraint min_count_in_map(std::string_view name, const node2count_t&);

} // namespace cluster
