/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/server/handlers/add_offsets_to_txn.h"
#include "kafka/server/handlers/add_partitions_to_txn.h"
#include "kafka/server/handlers/alter_client_quotas.h"
#include "kafka/server/handlers/alter_configs.h"
#include "kafka/server/handlers/alter_partition_reassignments.h"
#include "kafka/server/handlers/api_versions.h"
#include "kafka/server/handlers/create_acls.h"
#include "kafka/server/handlers/create_partitions.h"
#include "kafka/server/handlers/create_topics.h"
#include "kafka/server/handlers/delete_acls.h"
#include "kafka/server/handlers/delete_groups.h"
#include "kafka/server/handlers/delete_records.h"
#include "kafka/server/handlers/delete_topics.h"
#include "kafka/server/handlers/describe_acls.h"
#include "kafka/server/handlers/describe_client_quotas.h"
#include "kafka/server/handlers/describe_configs.h"
#include "kafka/server/handlers/describe_groups.h"
#include "kafka/server/handlers/describe_log_dirs.h"
#include "kafka/server/handlers/describe_producers.h"
#include "kafka/server/handlers/describe_transactions.h"
#include "kafka/server/handlers/end_txn.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/find_coordinator.h"
#include "kafka/server/handlers/heartbeat.h"
#include "kafka/server/handlers/incremental_alter_configs.h"
#include "kafka/server/handlers/init_producer_id.h"
#include "kafka/server/handlers/join_group.h"
#include "kafka/server/handlers/leave_group.h"
#include "kafka/server/handlers/list_groups.h"
#include "kafka/server/handlers/list_offsets.h"
#include "kafka/server/handlers/list_partition_reassignments.h"
#include "kafka/server/handlers/list_transactions.h"
#include "kafka/server/handlers/metadata.h"
#include "kafka/server/handlers/offset_commit.h"
#include "kafka/server/handlers/offset_delete.h"
#include "kafka/server/handlers/offset_fetch.h"
#include "kafka/server/handlers/offset_for_leader_epoch.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/handlers/sync_group.h"
#include "kafka/server/handlers/txn_offset_commit.h"

namespace kafka {
template<typename... Ts>
struct type_list {};

template<typename... Requests>
requires(KafkaApiHandler<Requests>, ...)
using make_request_types = type_list<Requests...>;

/*
 * This set of handlers defines what the kafka server supports. If you are
 * adding to this list, then you'll want to also update the list in
 * kafka/protocol/flex_versions.cc which define properties about the protocol
 * itself as shared between our client and server.
 */
using request_types = make_request_types<
  produce_handler,
  fetch_handler,
  list_offsets_handler,
  metadata_handler,
  offset_fetch_handler,
  offset_delete_handler,
  find_coordinator_handler,
  list_groups_handler,
  api_versions_handler,
  join_group_handler,
  heartbeat_handler,
  delete_records_handler,
  leave_group_handler,
  sync_group_handler,
  create_topics_handler,
  offset_commit_handler,
  describe_configs_handler,
  alter_configs_handler,
  delete_topics_handler,
  describe_groups_handler,
  sasl_handshake_handler,
  sasl_authenticate_handler,
  incremental_alter_configs_handler,
  delete_groups_handler,
  describe_acls_handler,
  describe_log_dirs_handler,
  create_acls_handler,
  delete_acls_handler,
  init_producer_id_handler,
  add_partitions_to_txn_handler,
  txn_offset_commit_handler,
  add_offsets_to_txn_handler,
  end_txn_handler,
  create_partitions_handler,
  offset_for_leader_epoch_handler,
  alter_partition_reassignments_handler,
  list_partition_reassignments_handler,
  describe_producers_handler,
  describe_transactions_handler,
  list_transactions_handler,
  alter_client_quotas_handler,
  describe_client_quotas_handler>;

template<typename... RequestTypes>
static constexpr size_t max_api_key(type_list<RequestTypes...>) {
    /// Black magic here is an overload of std::max() that takes an
    /// std::initializer_list
    return std::max({RequestTypes::api::key()...});
}
} // namespace kafka
