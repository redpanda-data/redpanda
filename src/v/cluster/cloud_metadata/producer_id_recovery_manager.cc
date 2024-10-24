/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"

#include "cluster/controller_service.h"
#include "cluster/health_monitor_types.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/record.h"

namespace cluster::cloud_metadata {

producer_id_recovery_manager::producer_id_recovery_manager(
  ss::sharded<cluster::members_table>& members_table,
  ss::sharded<rpc::connection_cache>& conn_cache,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator)
  : members_table_(members_table)
  , conn_cache_(conn_cache)
  , id_allocator_(id_allocator) {}

ss::future<cloud_metadata::error_outcome>
producer_id_recovery_manager::recover() const {
    vlog(clusterlog.info, "Recovering highest producer id");
    auto highest_pid_res = co_await get_cluster_highest_pid();
    if (highest_pid_res.has_error()) {
        co_return highest_pid_res.error();
    }
    auto highest_pid = highest_pid_res.value();
    if (highest_pid == model::producer_id{}) {
        vlog(clusterlog.info, "No valid producer ids found");
        co_return error_outcome::success;
    }
    vlog(clusterlog.info, "Found highest producer id {}", highest_pid);
    auto next_id = model::producer_id{highest_pid() + 1};
    auto reply = co_await id_allocator_.local().reset_next_id(
      next_id, timeout_);
    if (reply.ec != cluster::errc::success) {
        co_return error_outcome::rpc_error;
    }
    vlog(clusterlog.info, "Reset highest producer id to {}", next_id);
    co_return error_outcome::success;
}

ss::future<result<model::producer_id, error_outcome>>
producer_id_recovery_manager::get_node_highest_pid(
  const model::broker& broker) const {
    vlog(
      clusterlog.debug,
      "Looking for producer id on node {} ({})",
      broker.id(),
      broker.rpc_address());
    auto reply_res = co_await with_client<cluster::controller_client_protocol>(
      self_.id(),
      conn_cache_,
      broker.id(),
      broker.rpc_address(),
      rpc_tls_config_,
      timeout_,
      [this](controller_client_protocol c) mutable {
          return c
            .highest_producer_id(
              producer_id_lookup_request{}, rpc::client_opts(timeout_))
            .then(&rpc::get_ctx_data<producer_id_lookup_reply>);
      });
    if (reply_res.has_error()) {
        co_return error_outcome::rpc_error;
    }
    vlog(
      clusterlog.debug,
      "Found producer id {} on node {} ({})",
      reply_res.value().highest_producer_id,
      broker.id(),
      broker.rpc_address());
    co_return reply_res.value().highest_producer_id;
}

ss::future<result<model::producer_id, error_outcome>>
producer_id_recovery_manager::get_cluster_highest_pid() const {
    std::vector<ss::future<result<model::producer_id, error_outcome>>> futs;
    futs.reserve(members_table_.local().node_count());
    for (const auto& [node_id, meta] : members_table_.local().nodes()) {
        if (
          meta.state.get_membership_state()
          == model::membership_state::removed) {
            continue;
        }
        futs.emplace_back(get_node_highest_pid(meta.broker));
    }
    vlog(clusterlog.debug, "Looking for producer id on {} nodes", futs.size());
    auto pids = co_await ss::when_all_succeed(futs.begin(), futs.end());
    model::producer_id highest_pid;
    for (const auto& pid : pids) {
        if (pid.has_error()) {
            co_return pid.error();
        }
        highest_pid = std::max(pid.value(), highest_pid);
    }
    co_return highest_pid;
}

} // namespace cluster::cloud_metadata
