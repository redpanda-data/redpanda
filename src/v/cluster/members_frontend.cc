// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_frontend.h"

#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <absl/container/flat_hash_set.h>

#include <chrono>

namespace cluster {

members_frontend::members_frontend(
  ss::sharded<controller_stm>& stm,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<ss::abort_source>& as)
  : _self(config::shard_local_cfg().node_id())
  , _node_op_timeout(
      config::shard_local_cfg().node_management_operation_timeout_ms)
  , _stm(stm)
  , _connections(connections)
  , _leaders(leaders)
  , _partition_manager(partition_manager)
  , _as(as) {}

ss::future<> members_frontend::start() { return ss::now(); }

ss::future<> members_frontend::stop() { return ss::now(); }

ss::future<std::error_code>
members_frontend::finish_node_reallocations(model::node_id id) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_replicate_node_command<finish_reallocations_cmd>(
          id);
    }
    const std::chrono::duration timeout = _node_op_timeout;
    auto res = co_await _connections.local()
                 .with_node_client<cluster::controller_client_protocol>(
                   _self,
                   ss::this_shard_id(),
                   *leader,
                   timeout,
                   [id, timeout](controller_client_protocol cp) mutable {
                       return cp.finish_reallocation(
                         finish_reallocation_request{.id = id},
                         rpc::client_opts(
                           model::timeout_clock::now() + timeout));
                   });

    if (res.has_error()) {
        co_return res.error();
    }
    co_return res.value().data.error;
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

ss::future<members_frontend::get_under_replicated_reply>
members_frontend::get_under_replicated(model::node_id id) {
    if (id != _self()) {
        co_return get_under_replicated_reply{
          .under_replicated = co_await is_node_under_replicated(id),
          .error = errc::success};
    }

    constexpr model::node_id no_leader{-1};
    absl::flat_hash_set<model::node_id> leaders;
    const auto& parts = _partition_manager.local().partitions();
    for (const auto& part : parts) {
        leaders.insert(
          _leaders.local().get_leader(part.first).value_or(no_leader));
    }
    if (leaders.count(no_leader)) {
        co_return get_under_replicated_reply{true, errc::no_leader_controller};
    }
    leaders.erase(_self);

    auto get_remote_under_replicated = [this](model::node_id leader) {
        const auto expires = model::timeout_clock::now() + _node_op_timeout;
        const auto self = _self;
        return _connections.local()
          .with_node_client<cluster::controller_client_protocol>(
            self,
            ss::this_shard_id(),
            leader,
            _node_op_timeout,
            [self, expires](controller_client_protocol cp) mutable {
                return cp.get_under_replicated(
                  get_under_replicated_request{.id = self},
                  rpc::client_opts(expires));
            });
    };

    auto res = co_await ssx::parallel_transform(
      std::move(leaders), get_remote_under_replicated);

    // Treat errors as under replicated as we don't have confirmation that
    // it's not under replicated
    auto it = std::find_if(res.begin(), res.end(), [](const auto& res) {
        return res.has_error() || res.value().data.error != errc::success
               || res.value().data.under_replicated;
    });
    if (it != res.end()) {
        co_return get_under_replicated_reply{
          .under_replicated = true,
          .error = it->has_error() ? it->assume_error()
                                   : it->assume_value().data.error};
    }
    co_return get_under_replicated_reply{
      .under_replicated = false, .error = errc::success};
}

ss::future<bool>
members_frontend::is_node_under_replicated(model::node_id id) const {
    const auto any_partition_under_replicated =
      [id](const cluster::partition_manager& pm) {
          const auto& parts = pm.partitions();
          return std::any_of(
            parts.begin(), parts.end(), [&pm, id](const auto& part) {
                auto ur = pm.consensus_for(part.second->group())
                            ->is_under_replicated(id);
                // Failure is either not_leader, or node_not_found, so it's
                // not known to be under_replicated, return success on error.
                return ur.has_value() && ur.assume_value();
            });
      };

    co_return co_await _partition_manager.map_reduce0(
      any_partition_under_replicated, false, std::logical_or<>{});
}

} // namespace cluster
