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

#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_set.h>
#include <fmt/ostream.h>

namespace cluster {

hard_constraint_evaluator not_fully_allocated() {
    class impl : public hard_constraint_evaluator::impl {
    public:
        bool evaluate(const allocation_node& node) const final {
            return !node.is_full();
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "node must have empty allocations slots");
        }
    };

    return hard_constraint_evaluator(std::make_unique<impl>());
}

hard_constraint_evaluator is_active() {
    class impl : public hard_constraint_evaluator::impl {
    public:
        bool evaluate(const allocation_node& node) const final {
            return node.is_active();
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "node must be active");
        }
    };

    return hard_constraint_evaluator(std::make_unique<impl>());
}

hard_constraint_evaluator on_node(model::node_id id) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        explicit impl(model::node_id id)
          : _id(id) {}

        bool evaluate(const allocation_node& node) const final {
            return node.id() == _id;
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "with node id equal to {}", _id);
        }

    private:
        model::node_id _id;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(id));
}

hard_constraint_evaluator on_nodes(const std::vector<model::node_id>& ids) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        explicit impl(const std::vector<model::node_id>& ids)
          : _ids() {
            _ids.reserve(ids.size());
            for (auto& id : ids) {
                _ids.emplace(id);
            }
        }

        bool evaluate(const allocation_node& node) const final {
            return _ids.contains(node.id());
        }

        void print(std::ostream& o) const final {
            if (_ids.empty()) {
                fmt::print(o, "on nodes: []");
            }
            auto it = _ids.begin();
            fmt::print(o, "on nodes: [{}", *it);
            ++it;
            for (; it != _ids.end(); ++it) {
                fmt::print(o, ",{}", *it);
            }

            fmt::print(o, "]");
        }

    private:
        absl::flat_hash_set<model::node_id> _ids;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(ids));
}
hard_constraint_evaluator
distinct_from(const std::vector<model::broker_shard>& current) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        explicit impl(const std::vector<model::broker_shard>& r)
          : _replicas(r) {}

        bool evaluate(const allocation_node& node) const final {
            return std::all_of(
              _replicas.begin(),
              _replicas.end(),
              [&node](const model::broker_shard& bs) {
                  return bs.node_id != node.id();
              });
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "distinct from: {}", _replicas);
        }

    private:
        const std::vector<model::broker_shard>& _replicas;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(current));
}

hard_constraint_evaluator disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        impl(
          const double max_disk_usage_ratio,
          const size_t partition_size,
          const absl::flat_hash_map<model::node_id, node_disk_space>&
            node_disk_reports)
          : _max_disk_usage_ratio(max_disk_usage_ratio)
          , _partition_size(partition_size)
          , _node_disk_reports(node_disk_reports) {}

        bool evaluate(const allocation_node& node) const final {
            auto disk_it = _node_disk_reports.find(node.id());
            if (disk_it == _node_disk_reports.end()) {
                return false;
            } else {
                const auto& node_disk = disk_it->second;
                auto peak_disk_usage = node_disk.used + node_disk.assigned
                                       + _partition_size;
                return peak_disk_usage
                       < _max_disk_usage_ratio * node_disk.total;
            }
            return false;
        }

        void print(std::ostream& o) const final {
            fmt::print(
              o,
              "partition with size {} doesn't overfill disk",
              _partition_size);
        }

        const double _max_disk_usage_ratio;
        const size_t _partition_size;
        const absl::flat_hash_map<model::node_id, node_disk_space>&
          _node_disk_reports;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(
      max_disk_usage_ratio, partition_size, node_disk_reports));
}

soft_constraint_evaluator least_allocated() {
    class impl : public soft_constraint_evaluator::impl {
    public:
        uint64_t score(const allocation_node& node) const final {
            // we return 0 for fully allocated node and 10'000'000 for nodes
            // with maximum capacity available
            return (soft_constraint_evaluator::max_score
                    * node.partition_capacity())
                   / node.max_capacity();
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "least allocated node");
        }
    };

    return soft_constraint_evaluator(std::make_unique<impl>());
}

soft_constraint_evaluator
least_allocated_in_domain(const partition_allocation_domain domain) {
    struct impl : soft_constraint_evaluator::impl {
        explicit impl(partition_allocation_domain domain_)
          : domain(domain_) {}
        uint64_t score(const allocation_node& node) const final {
            return (soft_constraint_evaluator::max_score
                    * node.domain_partition_capacity(domain))
                   / node.max_capacity();
        }
        void print(std::ostream& o) const final {
            fmt::print(o, "least allocated node in domain {}", domain);
        }
        partition_allocation_domain domain;
    };

    vassert(
      domain != partition_allocation_domains::common,
      "Least allocated constraint within common domain not supported");
    return soft_constraint_evaluator(std::make_unique<impl>(domain));
}

hard_constraint_evaluator distinct_rack(
  const std::vector<model::broker_shard>& replicas,
  const allocation_state& state) {
    class impl : public hard_constraint_evaluator::impl {
    public:
        impl(
          const std::vector<model::broker_shard>& replicas,
          const allocation_state& state)
          : _replicas(replicas)
          , _state(state) {}
        bool evaluate(const allocation_node& node) const final {
            for (auto [node_id, shard] : _replicas) {
                auto rack = _state.get_rack_id(node_id);
                // replica has no rack assigned, any node will match
                if (!rack.has_value()) {
                    return true;
                }
                // rack is already in replica set
                if (rack.value() == node.rack()) {
                    return false;
                }
            }
            return true;
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "distinct rack");
        }

        const std::vector<model::broker_shard>& _replicas;
        const allocation_state& _state;
    };

    return hard_constraint_evaluator(std::make_unique<impl>(replicas, state));
}

soft_constraint_evaluator least_disk_filled(
  const double max_disk_usage_ratio,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports) {
    class impl : public soft_constraint_evaluator::impl {
    public:
        impl(
          const double max_disk_usage_ratio,
          const absl::flat_hash_map<model::node_id, node_disk_space>&
            node_disk_reports)
          : _max_disk_usage_ratio(max_disk_usage_ratio)
          , _node_disk_reports(node_disk_reports) {}
        uint64_t score(const allocation_node& node) const final {
            // we return 0 for node filled more or equal to max_disk_usage_ratio
            // and 10'000'000 for nodes empty disks
            auto disk_it = _node_disk_reports.find(node.id());
            if (disk_it == _node_disk_reports.end()) {
                return 0;
            } else {
                const auto& node_disk = disk_it->second;
                if (node_disk.total == 0) {
                    return 0;
                }
                auto peak_disk_usage_ratio = node_disk.peak_used_ratio();
                if (peak_disk_usage_ratio > _max_disk_usage_ratio) {
                    return 0;
                } else {
                    return uint64_t(
                      soft_constraint_evaluator::max_score
                      * ((_max_disk_usage_ratio - peak_disk_usage_ratio) / _max_disk_usage_ratio));
                }
            }
            return 0;
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "least filled disk");
        }

        const double _max_disk_usage_ratio;
        const absl::flat_hash_map<model::node_id, node_disk_space>&
          _node_disk_reports;
    };

    return soft_constraint_evaluator(
      std::make_unique<impl>(max_disk_usage_ratio, node_disk_reports));
}

soft_constraint_evaluator
make_soft_constraint(hard_constraint_evaluator hard_constraint) {
    class impl : public soft_constraint_evaluator::impl {
    public:
        explicit impl(hard_constraint_evaluator hard_constraint)
          : _hard_constraint(std::move(hard_constraint)) {}
        uint64_t score(const allocation_node& node) const final {
            return _hard_constraint.evaluate(node)
                     ? soft_constraint_evaluator::max_score
                     : 0;
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "soft constraint adapter of ({})", _hard_constraint);
        }

        const hard_constraint_evaluator _hard_constraint;
    };

    return soft_constraint_evaluator(
      std::make_unique<impl>(std::move(hard_constraint)));
}

} // namespace cluster
