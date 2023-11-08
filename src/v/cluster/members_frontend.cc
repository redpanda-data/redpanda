// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_frontend.h"

#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <chrono>

namespace cluster {

members_frontend::members_frontend(
  ss::sharded<controller_stm>& stm,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<ss::abort_source>& as)
  : _self(*config::node().node_id())
  , _node_op_timeout(
      config::shard_local_cfg().node_management_operation_timeout_ms)
  , _stm(stm)
  , _connections(connections)
  , _leaders(leaders)
  , _feature_table(feature_table)
  , _as(as) {}

ss::future<std::error_code> members_frontend::finish_node_reallocations(
  model::node_id id, std::optional<model::term_id> term) {
    co_return co_await do_replicate_node_command<finish_reallocations_cmd>(
      id, term);
}

ss::future<std::error_code>
members_frontend::decommission_node(model::node_id id) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_replicate_node_command<decommission_node_cmd>(id);
    }

    const std::chrono::duration timeout = _node_op_timeout;
    auto res = co_await _connections.local()
                 .with_node_client<cluster::controller_client_protocol>(
                   _self,
                   ss::this_shard_id(),
                   *leader,
                   timeout,
                   [id, timeout](controller_client_protocol cp) mutable {
                       return cp.decommission_node(
                         decommission_node_request{.id = id},
                         rpc::client_opts(
                           model::timeout_clock::now() + timeout));
                   });

    if (res.has_error()) {
        co_return res.error();
    }
    co_return res.value().data.error;
}

ss::future<std::error_code>
members_frontend::recommission_node(model::node_id id) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_replicate_node_command<recommission_node_cmd>(id);
    }
    std::chrono::duration timeout = _node_op_timeout;
    auto res = co_await _connections.local()
                 .with_node_client<cluster::controller_client_protocol>(
                   _self,
                   ss::this_shard_id(),
                   *leader,
                   timeout,
                   [id, timeout](controller_client_protocol cp) mutable {
                       return cp.recommission_node(
                         recommission_node_request{.id = id},
                         rpc::client_opts(
                           model::timeout_clock::now() + timeout));
                   });

    if (res.has_error()) {
        co_return res.error();
    }
    co_return res.value().data.error;
}

ss::future<std::error_code>
members_frontend::set_maintenance_mode(model::node_id id, bool enabled) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        co_return errc::no_leader_controller;
    }

    if (leader == _self) {
        co_return co_await replicate_and_wait(
          _stm,
          _as,
          maintenance_mode_cmd(id, enabled),
          _node_op_timeout + model::timeout_clock::now());
    }

    std::chrono::duration timeout = _node_op_timeout;
    auto res
      = co_await _connections.local()
          .with_node_client<cluster::controller_client_protocol>(
            _self,
            ss::this_shard_id(),
            *leader,
            timeout,
            [id, enabled, timeout](controller_client_protocol cp) mutable {
                return cp.set_maintenance_mode(
                  set_maintenance_mode_request{.id = id, .enabled = enabled},
                  rpc::client_opts(model::timeout_clock::now() + timeout));
            });

    if (res.has_error()) {
        co_return res.error();
    }

    co_return res.value().data.error;
}

ss::future<std::error_code> members_frontend::remove_node(model::node_id id) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    /**
     * There is no need to forward the request to current controller node as
     * nodes are removed only by the node currently being a controller leader.
     */
    if (leader != _self) {
        co_return errc::not_leader_controller;
    }
    co_return co_await do_replicate_node_command<remove_node_cmd>(id);
}

} // namespace cluster
