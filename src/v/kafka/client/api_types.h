/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/add_offsets_to_txn.h"
#include "kafka/protocol/add_partitions_to_txn.h"
#include "kafka/protocol/alter_client_quotas.h"
#include "kafka/protocol/alter_configs.h"
#include "kafka/protocol/alter_partition_reassignments.h"
#include "kafka/protocol/alter_user_scram_credentials.h"
#include "kafka/protocol/api_versions.h"
#include "kafka/protocol/create_acls.h"
#include "kafka/protocol/create_partitions.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/delete_acls.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/delete_records.h"
#include "kafka/protocol/delete_topics.h"
#include "kafka/protocol/describe_acls.h"
#include "kafka/protocol/describe_client_quotas.h"
#include "kafka/protocol/describe_cluster.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/describe_log_dirs.h"
#include "kafka/protocol/describe_producers.h"
#include "kafka/protocol/describe_transactions.h"
#include "kafka/protocol/describe_user_scram_credentials.h"
#include "kafka/protocol/end_txn.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/incremental_alter_configs.h"
#include "kafka/protocol/init_producer_id.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/list_partition_reassignments.h"
#include "kafka/protocol/list_transactions.h"
#include "kafka/protocol/messages.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_delete.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/offset_for_leader_epoch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/protocol/txn_offset_commit.h"

#include <variant>

namespace kafka::client {
namespace internal {
template<typename... Ts>
auto request_variant(type_list<Ts...>) {
    return std::variant<typename Ts::request_type...>();
}
template<typename... Ts>
auto response_variant(type_list<Ts...>) {
    return std::variant<typename Ts::response_type...>();
}
} // namespace internal

using request_t = decltype(internal::request_variant(request_types{}));
using response_t = decltype(internal::response_variant(request_types{}));

} // namespace kafka::client
