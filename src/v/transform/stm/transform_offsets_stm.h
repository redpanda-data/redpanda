/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/distributed_kv_stm.h"
#include "cluster/state_machine_registry.h"
#include "cluster/topic_table.h"

namespace transform {

using transform_offsets_stm_t = cluster::distributed_kv_stm<
  model::transform_offsets_key,
  model::transform_offsets_value,
  "transform_offsets_stm">;

class transform_offsets_stm_factory : public cluster::state_machine_factory {
public:
    explicit transform_offsets_stm_factory(ss::sharded<cluster::topic_table>&);
    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;

private:
    ss::sharded<cluster::topic_table>& _topics;
};
} // namespace transform
