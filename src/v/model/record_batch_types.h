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

#include "utils/named_type.h"

#include <cstdint>
#include <ostream>

namespace model {
enum class record_batch_type : int8_t {
    raft_data = 1,            // raft::data
    raft_configuration = 2,   // raft::configuration
    controller = 3,           // controller::*
    kvstore = 4,              // kvstore::*
    checkpoint = 5,           // checkpoint - used to achieve linearizable reads
    topic_management_cmd = 6, // controller topic command batch type
    ghost_batch = 7,          // ghost - used to fill gaps in raft recovery
    id_allocator = 8,         // id_allocator_stm::*
    tx_prepare = 9,           // tx_prepare_batch_type
    tx_fence = 10,            // tx_fence_batch_type
    tm_update = 11,           // tm_update_batch_type
    user_management_cmd = 12, // controller user management command batch type
    acl_management_cmd = 13,  // controller acl management command batch type
    group_prepare_tx = 14,    // group_prepare_tx_batch_type
    group_commit_tx = 15,     // group_commit_tx_batch_type
    group_abort_tx = 16,      // group_abort_tx_batch_type
    node_management_cmd = 17, // controller node management
    data_policy_management_cmd = 18, // data-policy management
    archival_metadata = 19,          // archival metadata updates
    cluster_config_cmd = 20,         // cluster config deltas and status
    feature_update = 21,             // Node logical versions updates
    cluster_bootstrap_cmd = 22,      // cluster bootsrap command
    version_fence = 23,              // version fence/epoch
};

std::ostream& operator<<(std::ostream& o, record_batch_type bt);

} // namespace model
