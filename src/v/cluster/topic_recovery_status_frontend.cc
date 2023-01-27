
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/topic_recovery_status_frontend.h"

#include "cloud_storage/topic_recovery_service.h"
#include "cluster/members_table.h"
#include "cluster/topic_recovery_status_rpc_service.h"
#include "rpc/connection_cache.h"

namespace {
// TODO (abhijat) config
constexpr ss::lowres_clock::duration rpc_timeout_ms{5s};
} // namespace

namespace cluster {

topic_recovery_status_frontend::topic_recovery_status_frontend(
  model::node_id node_id,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<members_table>& members)
  : _self{node_id}
  , _connections{connections}
  , _members{members} {}

ss::future<bool> topic_recovery_status_frontend::is_recovery_running(
  ss::sharded<cloud_storage::topic_recovery_service>& topic_recovery_service,
  skip_this_node skip_this_node) const {
    // Check local state first
    if (!skip_this_node && topic_recovery_service.local().is_active()) {
        co_return true;
    }

    // If local recovery is not running, check on the other nodes
    auto nodes = _members.local().node_ids();
    auto it = std::remove_if(
      nodes.begin(), nodes.end(), [this](const auto& node) {
          return node == _self;
      });
    auto result = co_await ssx::parallel_transform(
      nodes.begin(), it, [this](auto node_id) {
          return _connections.local()
            .with_node_client<topic_recovery_status_rpc_client_protocol>(
              _self,
              ss::this_shard_id(),
              node_id,
              rpc_timeout_ms,
              [](topic_recovery_status_rpc_client_protocol p) {
                  return p.get_status(
                    status_request{}, rpc::client_opts{rpc_timeout_ms});
              });
      });

    co_return std::transform_reduce(
      std::make_move_iterator(result.begin()),
      std::make_move_iterator(result.end()),
      false,
      std::logical_or{},
      [](auto&& node_result) {
          // If we failed to get status from a node, assume that recovery is not
          // active
          return node_result.has_value() && node_result.value().data.is_active;
      });
}

} // namespace cluster
