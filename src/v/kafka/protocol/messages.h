/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/protocol/schemata/add_offsets_to_txn_request.h"
#include "kafka/protocol/schemata/add_partitions_to_txn_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/create_acls_request.h"
#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/delete_acls_request.h"
#include "kafka/protocol/schemata/delete_groups_request.h"
#include "kafka/protocol/schemata/delete_records_request.h"
#include "kafka/protocol/schemata/delete_topics_request.h"
#include "kafka/protocol/schemata/describe_acls_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_configs_request.h"
#include "kafka/protocol/schemata/describe_groups_request.h"
#include "kafka/protocol/schemata/describe_log_dirs_request.h"
#include "kafka/protocol/schemata/describe_producers_request.h"
#include "kafka/protocol/schemata/describe_transactions_request.h"
#include "kafka/protocol/schemata/end_txn_request.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/find_coordinator_request.h"
#include "kafka/protocol/schemata/heartbeat_request.h"
#include "kafka/protocol/schemata/incremental_alter_configs_request.h"
#include "kafka/protocol/schemata/init_producer_id_request.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/protocol/schemata/leave_group_request.h"
#include "kafka/protocol/schemata/list_groups_request.h"
#include "kafka/protocol/schemata/list_offset_request.h"
#include "kafka/protocol/schemata/list_partition_reassignments_request.h"
#include "kafka/protocol/schemata/list_transactions_request.h"
#include "kafka/protocol/schemata/metadata_request.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/protocol/schemata/offset_delete_request.h"
#include "kafka/protocol/schemata/offset_fetch_request.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_request.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/schemata/sasl_authenticate_request.h"
#include "kafka/protocol/schemata/sasl_handshake_request.h"
#include "kafka/protocol/schemata/sync_group_request.h"
#include "kafka/protocol/schemata/txn_offset_commit_request.h"

#include <algorithm>

namespace kafka {

template<typename... Ts>
struct type_list {};

template<typename... Requests>
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  produce_api,
  fetch_api,
  list_offsets_api,
  metadata_api,
  offset_fetch_api,
  offset_delete_api,
  find_coordinator_api,
  list_groups_api,
  api_versions_api,
  join_group_api,
  heartbeat_api,
  delete_records_api,
  leave_group_api,
  sync_group_api,
  create_topics_api,
  offset_commit_api,
  describe_configs_api,
  alter_configs_api,
  delete_topics_api,
  describe_groups_api,
  sasl_handshake_api,
  sasl_authenticate_api,
  incremental_alter_configs_api,
  delete_groups_api,
  describe_acls_api,
  describe_log_dirs_api,
  create_acls_api,
  delete_acls_api,
  init_producer_id_api,
  add_partitions_to_txn_api,
  txn_offset_commit_api,
  add_offsets_to_txn_api,
  end_txn_api,
  create_partitions_api,
  offset_for_leader_epoch_api,
  alter_partition_reassignments_api,
  list_partition_reassignments_api,
  describe_producers_api,
  describe_transactions_api,
  list_transactions_api,
  alter_client_quotas_api,
  describe_client_quotas_api>;

} // namespace kafka
