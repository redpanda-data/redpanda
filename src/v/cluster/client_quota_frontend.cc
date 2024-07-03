// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_frontend.h"

#include "cluster/client_quota_serde.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/partition_leaders_table.h"
#include "rpc/connection_cache.h"

#include <seastar/core/future.hh>

namespace cluster::client_quota {

ss::future<cluster::errc> frontend::alter_quotas(
  alter_delta_cmd_data data, model::timeout_clock::time_point tout) {
    auto cluster_leader = _leaders.local().get_leader(model::controller_ntp);
    if (!cluster_leader) {
        return ss::make_ready_future<cluster::errc>(errc::no_leader_controller);
    } else if (*cluster_leader != _self) {
        return dispatch_alter_to_remote(
          *cluster_leader, std::move(data), tout - model::timeout_clock::now());
    } else {
        return do_alter_quotas(std::move(data), tout);
    }
}

ss::future<cluster::errc> frontend::dispatch_alter_to_remote(
  model::node_id cluster_leader,
  alter_delta_cmd_data cmd,
  model::timeout_clock::duration timeout) {
    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        cluster_leader,
        timeout,
        [cmd = std::move(cmd),
         timeout](controller_client_protocol client) mutable {
            return client
              .alter_client_quotas(
                alter_quotas_request{
                  .cmd_data = std::move(cmd), .timeout = timeout},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<alter_quotas_response>);
        })
      .then([](result<alter_quotas_response> r) {
          if (r.has_error()) {
              return map_update_interruption_error_code(r.error());
          }
          return r.value().ec;
      });
    ;
}

ss::future<cluster::errc> frontend::do_alter_quotas(
  alter_delta_cmd_data data, model::timeout_clock::time_point tout) {
    alter_quotas_delta_cmd cmd(0 /*unused*/, std::move(data));
    return replicate_and_wait(_stm, _as, std::move(cmd), tout)
      .then(map_update_interruption_error_code);
}

} // namespace cluster::client_quota
