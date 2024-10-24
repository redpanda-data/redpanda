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
#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/controller_backend.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <system_error>

namespace cluster {
/**
 * An entry point to read controller/cluster state
 */
class controller_api {
public:
    controller_api(
      model::node_id,
      ss::sharded<controller_backend>&,
      ss::sharded<topic_table>&,
      ss::sharded<shard_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<members_table>&,
      ss::sharded<partition_balancer_backend>&,
      ss::sharded<ss::abort_source>&);

    ss::future<result<std::vector<ntp_reconciliation_state>>>
      get_reconciliation_state(model::topic_namespace_view);

    ss::future<std::vector<ntp_reconciliation_state>>
      get_reconciliation_state(std::vector<model::ntp>);

    ss::future<ntp_reconciliation_state> get_reconciliation_state(model::ntp);

    ss::future<result<bool>> all_reconciliations_done(std::deque<model::ntp>);

    /**
     * API to access both remote and local state
     */
    ss::future<result<std::vector<ntp_reconciliation_state>>>
      get_reconciliation_state(
        model::node_id,
        std::vector<model::ntp>,
        model::timeout_clock::time_point);

    ss::future<result<ntp_reconciliation_state>> get_reconciliation_state(
      model::node_id, model::ntp, model::timeout_clock::time_point);

    // high level APIs
    ss::future<std::error_code> wait_for_topic(
      model::topic_namespace_view, model::timeout_clock::time_point);

    ss::future<result<std::vector<partition_reconfiguration_state>>>
      get_partitions_reconfiguration_state(
        std::vector<model::ntp>, model::timeout_clock::time_point);
    /**
     * Returns state of controller backend from each node in the cluster for
     * requested list of ntps. A global reconciliation status contains a state
     * of reconciliation loop execution from every replica that is part of an
     * ntp replica set.
     */
    ss::future<global_reconciliation_state> get_global_reconciliation_state(
      std::vector<model::ntp>, model::timeout_clock::time_point);

    ss::future<result<node_decommission_progress>>
      get_node_decommission_progress(
        model::node_id, model::timeout_clock::time_point);

    std::optional<ss::shard_id> shard_for(const raft::group_id& group) const;
    std::optional<ss::shard_id> shard_for(const model::ntp& ntp) const;

private:
    ss::future<std::optional<backend_operation>>
      get_current_op(model::ntp, ss::shard_id);

    ss::future<result<ss::chunked_fifo<model::ntp>>>
      get_decommission_allocation_failures(model::node_id);

    model::node_id _self;
    ss::sharded<controller_backend>& _backend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<health_monitor_frontend>& _health_monitor;
    ss::sharded<members_table>& _members;
    ss::sharded<partition_balancer_backend>& _partition_balancer;
    ss::sharded<ss::abort_source>& _as;
};
} // namespace cluster
