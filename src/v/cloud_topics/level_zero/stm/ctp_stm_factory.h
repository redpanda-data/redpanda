/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/state_machine_registry.h"

namespace cloud_topics::l0 {

class ctp_stm_factory : public cluster::state_machine_factory {
public:
    bool is_applicable_for(const storage::ntp_config& ntp_cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft,
      const cluster::stm_instance_config& cfg) final;
};

} // namespace cloud_topics::l0
