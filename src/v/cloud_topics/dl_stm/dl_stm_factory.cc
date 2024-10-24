// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm_factory.h"

#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/logger.h"

namespace experimental::cloud_topics {

bool dl_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    return ntp_cfg.cloud_topic_enabled();
}

void dl_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<cloud_topics::dl_stm>(
      cloud_topics::cd_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace experimental::cloud_topics
