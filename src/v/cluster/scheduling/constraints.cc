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
#include "cluster/scheduling/constraints.h"

#include "cluster/members_table.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"
#include "ssx/sformat.h"

#include <absl/container/flat_hash_set.h>
#include <fmt/ostream.h>

#include <ios>
#include <sstream>

namespace cluster {

hard_constraint not_fully_allocated() {
    class impl : public hard_constraint::impl {
    public:
        hard_constraint_evaluator make_evaluator(
          const allocated_partition& partition,
          std::optional<model::node_id> prev) const final {
            return [&partition, prev](const allocation_node& node) {
                bool will_add_allocation = node.id() != prev
                                           && !partition.is_original(node.id());
                return !node.is_full(partition.ntp(), will_add_allocation);
            };
        }

        ss::sstring name() const final {
            return "node must have empty allocations slots";
        }
    };

    return hard_constraint(std::make_unique<impl>());
}

hard_constraint is_active() {
    class impl : public hard_constraint::impl {
    public:
        hard_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id>) const final {
            return [](const allocation_node& node) { return node.is_active(); };
        }

        ss::sstring name() const final { return "node must be active"; }
    };

    return hard_constraint(std::make_unique<impl>());
}

hard_constraint on_node(model::node_id id) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(model::node_id id)
          : _id(id) {}

        hard_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id>) const final {
            return
              [this](const allocation_node& node) { return node.id() == _id; };
        }

        ss::sstring name() const final {
            return ssx::sformat("on node: {}", _id);
        }

    private:
        model::node_id _id;
    };

    return hard_constraint(std::make_unique<impl>(id));
}

hard_constraint on_nodes(const std::vector<model::node_id>& ids) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(const std::vector<model::node_id>& ids)
          : _ids() {
            _ids.reserve(ids.size());
            for (auto& id : ids) {
                _ids.emplace(id);
            }
        }
        hard_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id>) const final {
            return [this](const allocation_node& node) {
                return _ids.contains(node.id());
            };
        }

        ss::sstring name() const final {
            if (_ids.empty()) {
                return ssx::sformat("on nodes: []");
            }
            std::stringstream sstream;
            auto it = _ids.begin();
            fmt::print(sstream, "on nodes: [{}", *it);
            ++it;
            for (; it != _ids.end(); ++it) {
                fmt::print(sstream, ",{}", *it);
            }

            fmt::print(sstream, "]");
            return sstream.str();
        }

    private:
        absl::flat_hash_set<model::node_id> _ids;
    };

    return hard_constraint(std::make_unique<impl>(ids));
}

hard_constraint distinct_from(const replicas_t& replicas) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(const std::vector<model::broker_shard>& r)
          : _replicas(r) {}

        hard_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id>) const final {
            return [this](const allocation_node& node) {
                return std::all_of(
                  _replicas.begin(),
                  _replicas.end(),
                  [&node](const model::broker_shard& bs) {
                      return bs.node_id != node.id();
                  });
            };
        }

        ss::sstring name() const final {
            return ssx::sformat("distinct from: {}", _replicas);
        }

    private:
        const std::vector<model::broker_shard>& _replicas;
    };

    return hard_constraint(std::make_unique<impl>(replicas));
}

hard_constraint distinct_nodes() {
    class impl : public hard_constraint::impl {
    public:
        hard_constraint_evaluator make_evaluator(
          const allocated_partition& partition,
          std::optional<model::node_id> prev) const final {
            return [&partition, prev](const allocation_node& node) {
                if (prev == node.id()) {
                    // can just stay on the same node
                    return true;
                }
                return std::all_of(
                  partition.replicas().begin(),
                  partition.replicas().end(),
                  [&node](const model::broker_shard& bs) {
                      return bs.node_id != node.id();
                  });
            };
        }

        ss::sstring name() const final {
            return ssx::sformat("distinct nodes");
        }
    };

    return hard_constraint(std::make_unique<impl>());
}

hard_constraint disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports) {
    class impl : public hard_constraint::impl {
    public:
        impl(
          const double max_disk_usage_ratio,
          const size_t partition_size,
          const absl::flat_hash_map<model::node_id, node_disk_space>&
            node_disk_reports)
          : _max_disk_usage_ratio(max_disk_usage_ratio)
          , _partition_size(partition_size)
          , _node_disk_reports(node_disk_reports) {}

        hard_constraint_evaluator make_evaluator(
          const allocated_partition& partition,
          std::optional<model::node_id> prev) const final {
            return [this, &partition, prev](const allocation_node& node) {
                auto disk_it = _node_disk_reports.find(node.id());
                if (disk_it == _node_disk_reports.end()) {
                    return false;
                }
                const auto& node_disk = disk_it->second;
                auto peak_disk_usage = node_disk.used + node_disk.assigned;
                if (node.id() != prev && !partition.is_original(node.id())) {
                    peak_disk_usage += _partition_size;
                }
                return peak_disk_usage
                       < _max_disk_usage_ratio * node_disk.total;
            };
        }

        ss::sstring name() const final {
            return ssx::sformat(
              "partition with size of {} doesn't overfill disk",
              _partition_size);
        }

        const double _max_disk_usage_ratio;
        const size_t _partition_size;
        const absl::flat_hash_map<model::node_id, node_disk_space>&
          _node_disk_reports;
    };

    return hard_constraint(std::make_unique<impl>(
      max_disk_usage_ratio, partition_size, node_disk_reports));
}

soft_constraint max_final_capacity() {
    struct impl : soft_constraint::impl {
        soft_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id> prev) const final {
            return [prev](const allocation_node& node) {
                auto count = node.final_partitions();
                if (prev != node.id()) {
                    count += 1;
                }

                // we return 0 for fully allocated node and 10'000'000 for
                // nodes with maximum capacity available
                auto final_capacity = node.max_capacity()
                                      - std::min(node.max_capacity(), count);
                return (soft_constraint::max_score * final_capacity)
                       / node.max_capacity();
            };
        }

        ss::sstring name() const final { return "max final capacity"; }
    };

    return soft_constraint(std::make_unique<impl>());
}

soft_constraint least_disk_filled(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports) {
    class impl : public soft_constraint::impl {
    public:
        impl(
          const double max_disk_usage_ratio,
          const size_t partition_size,
          const absl::flat_hash_map<model::node_id, node_disk_space>&
            node_disk_reports)
          : _max_disk_usage_ratio(max_disk_usage_ratio)
          , _partition_size(partition_size)
          , _node_disk_reports(node_disk_reports) {}

        soft_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id> prev) const final {
            return [this, prev](const allocation_node& node) -> uint64_t {
                // we return 0 for node filled more or equal to
                // max_disk_usage_ratio
                // and 10'000'000 for nodes empty disks
                auto disk_it = _node_disk_reports.find(node.id());
                if (disk_it == _node_disk_reports.end()) {
                    return 0;
                }
                const auto& node_disk = disk_it->second;
                if (node_disk.total == 0) {
                    return 0;
                }

                auto final_used = node_disk.used + node_disk.assigned
                                  - node_disk.released;
                if (node.id() != prev) {
                    final_used += _partition_size;
                }
                auto final_ratio = double(final_used) / node_disk.total;

                if (final_ratio > _max_disk_usage_ratio) {
                    return 0;
                }
                return uint64_t(
                  soft_constraint::max_score
                  * ((_max_disk_usage_ratio - final_ratio) / _max_disk_usage_ratio));
            };
        }

        ss::sstring name() const final { return "least filled disk"; }

        const double _max_disk_usage_ratio;
        const size_t _partition_size;
        const absl::flat_hash_map<model::node_id, node_disk_space>&
          _node_disk_reports;
    };

    return soft_constraint(std::make_unique<impl>(
      max_disk_usage_ratio, partition_size, node_disk_reports));
}

soft_constraint distinct_rack_preferred(const members_table& members) {
    return distinct_labels_preferred(
      rack_label.data(),
      [&members](model::node_id id) { return members.get_node_rack_id(id); });
}

soft_constraint
min_count_in_map(std::string_view name, const node2count_t& node2count) {
    struct impl : soft_constraint::impl {
        explicit impl(std::string_view name, const node2count_t& node2count)
          : _name(name)
          , _node2count(node2count) {}

        soft_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id> prev) const final {
            return [this, prev](const allocation_node& node) {
                size_t count = 0;
                if (auto it = _node2count.find(node.id());
                    it != _node2count.end()) {
                    count = it->second;
                }
                if (node.id() != prev) {
                    count += 1;
                }

                if (count > node.max_capacity()()) {
                    return uint64_t(0);
                }
                return soft_constraint::max_score
                       - count * soft_constraint::max_score
                           / node.max_capacity();
            };
        }

        ss::sstring name() const final { return {_name.begin(), _name.end()}; }

        std::string_view _name;
        const node2count_t& _node2count;
    };

    return soft_constraint(std::make_unique<impl>(name, node2count));
}

} // namespace cluster
