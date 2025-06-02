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

#include "recovery_rpc_handler.h"

#include "cluster/types.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/types.h"

#include <seastar/coroutine/as_future.hh>

#include <optional>

namespace raft {

ss::future<reset_learner_state_reply>
recovery_rpc_handler::do_reset_learner_state(
  reset_learner_state_request, cluster::consensus_ptr c) {
    if (!c) {
        co_return reset_learner_state_reply{};
    }

    auto f = co_await ss::coroutine::as_future(c->clear_state());
    if (f.failed()) {
        f.ignore_ready_future();
        co_return reset_learner_state_reply{};
    }

    co_return reset_learner_state_reply{
      .success = raft::reset_learner_state_reply::is_success::yes};
}

ss::future<reset_learner_state_reply> recovery_rpc_handler::reset_learner_state(
  reset_learner_state_request req, rpc::streaming_context&) {
    return dispatch_request(req, &recovery_rpc_handler::do_reset_learner_state);
}

} // namespace raft
