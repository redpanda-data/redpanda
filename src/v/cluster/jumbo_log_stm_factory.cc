// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/jumbo_log_stm_factory.h"

#include "cluster/jumbo_log_stm.h"
#include "consensus.h"
#include "logger.h"
#include "model/namespace.h"
#include "state_machine_manager.h"
#include "storage/ntp_config.h"

namespace cluster {

bool jumbo_log_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return cfg.ntp() == model::jumbo_log_ntp;
}

void jumbo_log_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    builder.create_stm<jumbo_log_stm>(clusterlog, raft);
}
} // namespace cluster
