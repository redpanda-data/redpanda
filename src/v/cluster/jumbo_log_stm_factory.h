/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "raft/state_machine.h"
#include "state_machine_manager.h"
#include "state_machine_registry.h"
#include "storage/ntp_config.h"

namespace cluster {

class jumbo_log_stm_factory : public state_machine_factory {
public:
    jumbo_log_stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;
};

} // namespace cluster
