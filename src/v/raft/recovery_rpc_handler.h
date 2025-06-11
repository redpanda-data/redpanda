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

#pragma once

#include "cluster/controller_api.h"
#include "cluster/types.h"
#include "raft/fundamental.h"
#include "raft/recovery_rpc_service.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>

#include <type_traits>

namespace raft {

class recovery_rpc_handler final : public recovery_rpc_service {
public:
    recovery_rpc_handler(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<cluster::controller_api>& api)
      : recovery_rpc_service(sg, ssg)
      , _api(api) {}

    ss::future<reset_learner_state_reply> reset_learner_state(
      reset_learner_state_request, rpc::streaming_context&) final;

private:
    static ss::future<reset_learner_state_reply> do_reset_learner_state(
      reset_learner_state_request, cluster::consensus_ptr);

    ss::sharded<cluster::controller_api>& _api;
};

} // namespace raft
