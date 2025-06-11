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

#include "raft/recovery_client_protocol.h"
#include "raft/recovery_rpc_types.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

#include <chrono>

namespace raft {

class recovery_rpc_client final : public recovery_client_protocol::impl {
public:
    ~recovery_rpc_client() noexcept override = default;
    explicit recovery_rpc_client(
      model::node_id self, ss::sharded<rpc::connection_cache>& ccache)
      : _self(self)
      , _ccache(ccache) {}

    ss::future<result<reset_learner_state_reply>> reset_learner_state(
      model::node_id, const model::ntp&, std::chrono::milliseconds) final;

private:
    model::node_id _self;
    ss::sharded<rpc::connection_cache>& _ccache;
};

} // namespace raft
