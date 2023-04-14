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
        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [](const allocation_node& node) { return !node.is_full(); };
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
        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
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

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
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
        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
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

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
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
        hard_constraint_evaluator
        make_evaluator(const replicas_t& current_replicas) const final {
            return [&current_replicas](const allocation_node& node) {
                return std::all_of(
                  current_replicas.begin(),
                  current_replicas.end(),
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

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [this](const allocation_node& node) {
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

soft_constraint least_allocated() {
    class impl : public soft_constraint::impl {
    public:
        soft_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [](const allocation_node& node) {
                // we return 0 for fully allocated node and 10'000'000 for nodes
                // with maximum capacity available
                return (soft_constraint::max_score * node.partition_capacity())
                       / node.max_capacity();
            };
        }

        ss::sstring name() const final { return "least allocated node"; }
    };

    return soft_constraint(std::make_unique<impl>());
}

soft_constraint
least_allocated_in_domain(const partition_allocation_domain domain) {
    struct impl : soft_constraint::impl {
        explicit impl(partition_allocation_domain domain_)
          : domain(domain_) {}

        soft_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [this](const allocation_node& node) {
                return (soft_constraint::max_score
                        * node.domain_partition_capacity(domain))
                       / node.max_capacity();
            };
        }

        ss::sstring name() const final {
            return ssx::sformat("least allocated node in domain {}", domain);
        }
        partition_allocation_domain domain;
    };

    vassert(
      domain != partition_allocation_domains::common,
      "Least allocated constraint within common domain not supported");
    return soft_constraint(std::make_unique<impl>(domain));
}

soft_constraint least_disk_filled(
  const double max_disk_usage_ratio,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports) {
    class impl : public soft_constraint::impl {
    public:
        impl(
          const double max_disk_usage_ratio,
          const absl::flat_hash_map<model::node_id, node_disk_space>&
            node_disk_reports)
          : _max_disk_usage_ratio(max_disk_usage_ratio)
          , _node_disk_reports(node_disk_reports) {}

        soft_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [this](const allocation_node& node) -> uint64_t {
                // we return 0 for node filled more or equal to
                // max_disk_usage_ratio
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
                          soft_constraint::max_score
                          * ((_max_disk_usage_ratio - peak_disk_usage_ratio) / _max_disk_usage_ratio));
                    }
                }
                return 0;
            };
        }

        ss::sstring name() const final { return "least filled disk"; }

        const double _max_disk_usage_ratio;
        const absl::flat_hash_map<model::node_id, node_disk_space>&
          _node_disk_reports;
    };

    return soft_constraint(
      std::make_unique<impl>(max_disk_usage_ratio, node_disk_reports));
}

soft_constraint make_soft_constraint(hard_constraint constraint) {
    class impl : public soft_constraint::impl {
    public:
        explicit impl(hard_constraint constraint)
          : _hard_constraint(std::move(constraint)) {}

        soft_constraint_evaluator
        make_evaluator(const replicas_t& replicas) const final {
            auto ev = _hard_constraint.make_evaluator(replicas);
            return
              [ev = std::move(ev)](const allocation_node& node) -> uint64_t {
                  return ev(node) ? soft_constraint::max_score : 0;
              };
        }

        ss::sstring name() const final {
            return ssx::sformat(
              "soft constraint adapter of ({})", _hard_constraint);
        }

        const hard_constraint _hard_constraint;
    };

    return soft_constraint(std::make_unique<impl>(std::move(constraint)));
}

soft_constraint distinct_rack_preferred(const members_table& members) {
    return distinct_labels_preferred(
      rack_label.data(),
      [&members](model::node_id id) { return members.get_node_rack_id(id); });
}

} // namespace cluster
