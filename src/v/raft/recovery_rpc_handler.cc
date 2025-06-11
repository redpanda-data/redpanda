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

#include "cluster/controller_api.h"
#include "cluster/types.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/types.h"

#include <seastar/coroutine/as_future.hh>

#include <optional>

namespace raft {

ss::future<reset_learner_state_reply> recovery_rpc_handler::reset_learner_state(
  reset_learner_state_request req, rpc::streaming_context&) {
    auto ec = co_await _api.local().remake_partition(req.ntp);
    std::cout << "error code is " << ec << '\n';
    co_return reset_learner_state_reply{};
}

} // namespace raft
