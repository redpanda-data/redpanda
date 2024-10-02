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
    tx_tm_hosted_trasactions = 24,   // tx_tm_hosted_trasactions_batch_type
    prefix_truncate = 25,            // log prefix truncation type
    plugin_update = 26,              // Wasm plugin update
    tx_registry = 27,                // tx_registry_batch_type
    cluster_recovery_cmd = 28,       // cluster recovery command
    compaction_placeholder
    = 29, // place holder for last batch in a segment that was aborted
    role_management_cmd = 30, // role management command
    client_quota = 31,        // client quota command
    data_migration_cmd = 32,  // data migration manipulation command
    group_fence_tx = 33,      // fence batch in group transactions
    partition_properties_update
    = 34, // special batch type used to update partition properties
    datalake_coordinator = 35, // datalake::coordinator::*
    dl_placeholder = 36,       // placeholder batch type used by cloud topics
    dl_stm_command = 37,       // dl_stm command batch
    MAX = dl_stm_command,
};

std::ostream& operator<<(std::ostream& o, record_batch_type bt);

// The set of batch types that may appear in a data partition that aren't
// assigned a new translated offset. When translated, such batches are given an
// offset matching the next batch of type outside this set.
//
// Put simply, batches of these types do not increment the offset that would be
// returned upon translating offsets for Kafka fetches.
inline std::vector<model::record_batch_type> offset_translator_batch_types() {
    return {
      model::record_batch_type::raft_configuration,
      model::record_batch_type::archival_metadata,
      model::record_batch_type::version_fence,
      model::record_batch_type::prefix_truncate,
      model::record_batch_type::partition_properties_update};
}

} // namespace model
