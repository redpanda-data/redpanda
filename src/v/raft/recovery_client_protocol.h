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

#include "base/outcome.h"
#include "raft/recovery_rpc_types.h"

namespace raft {

class recovery_client_protocol {
public:
    ~recovery_client_protocol() noexcept = default;
    struct impl {
        virtual ~impl() noexcept = default;

        virtual ss::future<result<reset_learner_state_reply>>
          reset_learner_state(
            model::node_id, group_id, std::chrono::milliseconds)
          = 0;
    };

private:
    ss::shared_ptr<impl> _impl;

public:
    explicit recovery_client_protocol(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}

    ss::future<result<reset_learner_state_reply>> reset_learner_state(
      model::node_id node, group_id group, std::chrono::milliseconds timeout) {
        return _impl->reset_learner_state(node, group, timeout);
    }
};

} // namespace raft
