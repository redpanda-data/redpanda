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

soft_constraint_evaluator distinct_rack(
  const std::vector<model::broker_shard>& replicas,
  const allocation_state& state) {
    class impl : public soft_constraint_evaluator::impl {
    public:
        impl(
          const std::vector<model::broker_shard>& replicas,
          const allocation_state& state)
          : _replicas(replicas)
          , _state(state) {}
        uint64_t score(const allocation_node& node) const final {
            // score node as 1 if the rack of the node is different
            // score as 0 otherwise
            for (auto [node_id, shard] : _replicas) {
                auto rack = _state.get_rack_id(node_id);
                if (!rack.has_value()) {
                    return 1;
                }
                if (rack.value() == node.rack()) {
                    return 0;
                }
            }
            return 1;
        }

        void print(std::ostream& o) const final {
            fmt::print(o, "distinct rack");
        }

        const std::vector<model::broker_shard>& _replicas;
        const allocation_state& _state;
    };

    return soft_constraint_evaluator(std::make_unique<impl>(replicas, state));
}

} // namespace cluster
