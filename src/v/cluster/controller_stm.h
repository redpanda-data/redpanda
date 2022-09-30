/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/config_manager.h"
#include "cluster/controller_log_limiter.h"
#include "cluster/data_policy_manager.h"
#include "cluster/feature_backend.h"
#include "cluster/members_manager.h"
#include "cluster/security_manager.h"
#include "cluster/topic_updates_dispatcher.h"
#include "raft/mux_state_machine.h"

#include <utility>

namespace cluster {

// single instance
class controller_stm
  : public raft::mux_state_machine<
      topic_updates_dispatcher,
      security_manager,
      members_manager,
      data_policy_manager,
      config_manager,
      feature_backend> {
public:
    controller_stm(
      limiter_configuration limiter_conf,
      ss::logger& logger,
      raft::consensus* c,
      raft::persistent_last_applied persist,
      topic_updates_dispatcher& tud,
      security_manager& sm,
      members_manager& mm,
      data_policy_manager& dpm,
      config_manager& cm,
      feature_backend& fb)
      : mux_state_machine(logger, c, persist, tud, sm, mm, dpm, cm, fb)
      , _limiter(std::move(limiter_conf)) {}

    controller_stm(controller_stm&&) = delete;
    controller_stm(const controller_stm&) = delete;
    controller_stm& operator=(controller_stm&&) = delete;
    controller_stm& operator=(const controller_stm&) = delete;
    ~controller_stm() = default;

    template<typename Cmd>
    ss::future<bool> throttle(Cmd& cmd, ss::sharded<ss::abort_source>& as) {
        return _limiter.throttle(cmd, as);
    }

private:
    controller_log_limiter _limiter;
};

static constexpr ss::shard_id controller_stm_shard = 0;

} // namespace cluster
