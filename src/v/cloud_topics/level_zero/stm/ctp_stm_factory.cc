/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_factory.h"

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "model/namespace.h"

namespace cloud_topics::l0 {

bool ctp_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    if (ntp_cfg.is_read_replica_mode_enabled()) {
        return false;
    }
    if (ntp_cfg.cloud_topic_enabled()) {
        return true;
    }
    // Pre-install an (idle) ctp_stm on user-topic partitions when cloud
    // topics are available to support migration to cloud/tsv2.
    return config::shard_local_cfg().cloud_storage_enabled()
           && model::is_user_topic(ntp_cfg.ntp());
}

void ctp_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<cloud_topics::ctp_stm>(
      cloud_topics::cd_log, raft);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cloud_topics::l0
