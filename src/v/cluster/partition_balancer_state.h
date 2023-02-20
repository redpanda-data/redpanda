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

#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/metadata.h"

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
      ss::sharded<partition_allocator>&);

    const topic_table& topics() const { return _topic_table; }

    const members_table& members() const { return _members_table; }

    const absl::btree_set<model::ntp>&
    ntps_with_broken_rack_constraint() const {
        return _ntps_with_broken_rack_constraint;
    }

    /// Called when the replica set of an ntp changes. Note that this doesn't
    /// account for in-progress moves - the function is called only once when
    /// the move is started.
    void handle_ntp_update(
      const model::ns&,
      const model::topic&,
      model::partition_id,
      const std::vector<model::broker_shard>& prev,
      const std::vector<model::broker_shard>& next);

    ss::future<> apply_snapshot(const controller_snapshot&);

private:
    struct probe {
        explicit probe(const partition_balancer_state&);

        void setup_metrics(ss::metrics::metric_groups&);

        const partition_balancer_state& _parent;
        ss::metrics::metric_groups _metrics;
        ss::metrics::metric_groups _public_metrics;
    };

private:
    topic_table& _topic_table;
    members_table& _members_table;
    partition_allocator& _partition_allocator;
    absl::btree_set<model::ntp> _ntps_with_broken_rack_constraint;
    probe _probe;
};

} // namespace cluster
