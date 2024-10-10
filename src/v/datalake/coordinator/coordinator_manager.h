/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "datalake/coordinator/coordinator.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/notification.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace datalake::coordinator {

// Manages the lifecycle of datalake coordinators, each of which operate on a
// single partition of the control topic.
class coordinator_manager {
public:
    coordinator_manager(
      model::node_id self,
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::partition_manager>&);

    ss::future<> start();
    ss::future<> stop();

    ss::lw_shared_ptr<coordinator> get(const model::ntp&) const;

private:
    void start_managing(cluster::partition&);
    void stop_managing(const model::ntp&);
    void notify_leadership_change(
      raft::group_id group,
      model::term_id term,
      std::optional<model::node_id> leader_id);

    ss::gate gate_;
    model::node_id self_;
    raft::group_manager& gm_;
    cluster::partition_manager& pm_;

    std::optional<cluster::notification_id_type> manage_notifications_;
    std::optional<cluster::notification_id_type> unmanage_notifications_;
    std::optional<raft::group_manager_notification_id>
      leadership_notifications_;

    absl::btree_map<model::ntp, ss::lw_shared_ptr<coordinator>> coordinators_;
};

} // namespace datalake::coordinator
