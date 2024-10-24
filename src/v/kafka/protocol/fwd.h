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

namespace kafka {

struct alter_configs_request;
struct alter_configs_response;
struct alter_client_quotas_request;
struct api_versions_request;
struct api_versions_response;
struct create_topics_request;
struct create_topics_response;
struct delete_groups_request;
struct delete_groups_response;
struct delete_topics_request;
struct delete_topics_response;
struct describe_configs_request;
struct describe_configs_response;
struct describe_client_quotas_request;
struct describe_groups_request;
struct describe_groups_request;
struct find_coordinator_response;
struct find_coordinator_request;
struct heartbeat_request;
struct heartbeat_response;
struct incremental_alter_configs_request;
struct incremental_alter_configs_response;
struct init_producer_id_request;
struct init_producer_id_response;
struct join_group_request;
struct join_group_response;
struct leave_group_request;
struct leave_group_response;
struct list_groups_request;
struct list_groups_response;
struct list_offsets_request;
struct list_offsets_response;
struct metadata_request;
struct metadata_response;
struct offset_commit_request;
struct offset_commit_response;
struct txn_offset_commit_request;
struct txn_offset_commit_response;
struct offset_fetch_request;
struct offset_fetch_response;
struct produce_request;
struct produce_response;
namespace protocol {
class encoder;
}
struct sasl_authenticate_request;
struct sasl_authenticate_response;
struct sasl_handshake_request;
struct sasl_handshake_response;
struct sync_group_request;
struct sync_group_response;

} // namespace kafka
