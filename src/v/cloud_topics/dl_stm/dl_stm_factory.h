// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/state_machine_registry.h"

namespace experimental::cloud_topics {

class dl_stm_factory : public cluster::state_machine_factory {
public:
    dl_stm_factory() = default;

    bool is_applicable_for(const storage::ntp_config& ntp_cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;
};

} // namespace experimental::cloud_topics
