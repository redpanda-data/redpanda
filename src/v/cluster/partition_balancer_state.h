/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "cluster/types.h"
#include "container/chunked_vector.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/stable_iterator_adaptor.h"

#include <seastar/core/sharded.hh>

namespace cluster {

/// Class that stores state that is needed for functioning of the partition
/// balancer. It is updated from the controller log (via
/// topic_updates_dispatcher)
class partition_balancer_state {
public:
    partition_balancer_state(
      ss::sharded<topic_table>&,
      ss::sharded<members_table>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<node_status_table>&);
    partition_balancer_state(const partition_balancer_state&) = delete;
    partition_balancer_state(partition_balancer_state&&) = delete;
    partition_balancer_state&
    operator=(const partition_balancer_state&) = delete;
    partition_balancer_state& operator=(partition_balancer_state&&) = delete;
    ~partition_balancer_state() = default;

    ss::future<> stop();

    topic_table& topics() const { return _topic_table; }

    members_table& members() const { return _members_table; }

    node_status_table& node_status() const { return _node_status; }

    bool is_rack_awareness_enabled() const;

    const absl::btree_set<model::ntp>&
    ntps_with_broken_rack_constraint() const {
        return _ntps_with_broken_rack_constraint;
    }

    auto ntps_with_broken_rack_constraint_it_begin() const {
        return stable_iterator<
          absl::btree_set<model::ntp>::const_iterator,
          model::revision_id>(
          [&]() { return _ntps_with_broken_rack_constraint_revision; },
          _ntps_with_broken_rack_constraint.begin());
    }

    auto ntps_with_broken_rack_constraint_it_end() const {
        return stable_iterator<
          absl::btree_set<model::ntp>::const_iterator,
          model::revision_id>(
          [&]() { return _ntps_with_broken_rack_constraint_revision; },
          _ntps_with_broken_rack_constraint.end());
    }

    /// Topics whose `replicas_preference` is currently set to a non-"none"
    /// type. Seeded by ensure_pinning_cache_seeded() from a full topic_table
    /// scan on first use, then maintained incrementally via topic_table
    /// delta notifications. Only populated on shard 0 (the balancer shard).
    const absl::btree_set<model::topic_namespace>&
    topics_with_replica_pinning() const {
        return _topics_with_replica_pinning;
    }

    /// Bootstrap the pinning cache from topic_table if not yet seeded.
    /// Idempotent; cheap after the first call within a term.
    void ensure_pinning_cache_seeded();

    /// Invalidate the pinning cache so the next
    /// ensure_pinning_cache_seeded() call re-scans topic_table. Also clears
    /// the current contents so a caller reading between reset and re-seed
    /// does not observe stale data. Call on raft0 term change or when
    /// topic_table is rebuilt from a controller snapshot.
    void reset_pinning_cache() {
        _pinning_cache_seeded = false;
        _topics_with_replica_pinning.clear();
    }

    /// Called when the replica set of an ntp changes. Note that this doesn't
    /// account for in-progress moves - the function is called only once when
    /// the move is started.
    void handle_ntp_move_begin_or_cancel(
      const model::ns&,
      const model::topic&,
      model::partition_id,
      const std::vector<model::broker_shard>& prev,
      const std::vector<model::broker_shard>& next);

    void handle_ntp_move_finish(
      const model::ntp& ntp, const std::vector<model::broker_shard>& replicas);
    void handle_ntp_delete(const model::ntp& ntp);

    void add_node_to_rebalance(model::node_id id) {
        _nodes_to_rebalance.insert(id);
    }

    void remove_node_to_rebalance(model::node_id id) {
        _nodes_to_rebalance.erase(id);
    }

    const auto& nodes_to_rebalance() const { return _nodes_to_rebalance; }

    ss::future<> apply_snapshot(const controller_snapshot&);

private:
    struct probe {
        explicit probe(const partition_balancer_state&);

        void setup_metrics(metrics::metric_groups_base&);

        const partition_balancer_state& _parent;
        metrics::internal_metric_groups _metrics;
        metrics::public_metric_groups _public_metrics;
    };

private:
    topic_table& _topic_table;
    members_table& _members_table;
    partition_allocator& _partition_allocator;
    node_status_table& _node_status;
    absl::btree_set<model::ntp> _ntps_with_broken_rack_constraint;
    // revision increment to be paired with all updates
    // _ntps_with_broken_rack_constraint set. Relied upon by the iterator.
    model::revision_id _ntps_with_broken_rack_constraint_revision;
    absl::flat_hash_set<model::node_id> _nodes_to_rebalance;
    probe _probe;

    absl::btree_set<model::topic_namespace> _topics_with_replica_pinning;
    bool _pinning_cache_seeded{false};
    cluster::notification_id_type _topic_deltas_handle{
      cluster::notification_id_type_invalid};

    void handle_topic_deltas(const chunked_vector<topic_table_topic_delta>&);
};

} // namespace cluster
