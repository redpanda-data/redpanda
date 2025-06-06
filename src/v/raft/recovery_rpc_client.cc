/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/recovery_rpc_client.h"

#include "base/outcome.h"
#include "model/timeout_clock.h"
#include "raft/recovery_rpc_service.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"

#include <chrono>

namespace raft {

ss::future<result<reset_learner_state_reply>>
recovery_rpc_client::reset_learner_state(
  model::node_id node, group_id group, std::chrono::milliseconds timeout) {
    return _ccache.local()
      .with_node_client<recovery_rpc_client_protocol>(
        _self,
        ss::this_shard_id(),
        node,
        timeout,
        [group, timeout](recovery_rpc_client_protocol cp) mutable {
            return cp.reset_learner_state(
              reset_learner_state_request{.id = group},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<reset_learner_state_reply>);
}

} // namespace raft
