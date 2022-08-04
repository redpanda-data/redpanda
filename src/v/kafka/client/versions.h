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
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/types.h"

namespace kafka::client {
static constexpr api_version unlisted_api = api_version(-2);

template<typename... Ts>
struct type_list {};

template<typename... Requests>
using make_supported_request_types = type_list<Requests...>;

template<typename T, api_version::type U>
struct request_version {
    using type = T;
    static constexpr api_key key = T::api_type::key;
    static constexpr api_version supported_version = api_version(U);
};

using supported_requests = make_supported_request_types<
  request_version<offset_fetch_request, 4>,
  request_version<fetch_request, 10>,
  request_version<list_offsets_request, 3>,
  request_version<produce_request, 7>,
  request_version<offset_commit_request, 7>,
  request_version<describe_groups_request, 2>,
  request_version<heartbeat_request, 3>,
  request_version<join_group_request, 4>,
  request_version<sync_group_request, 3>,
  request_version<leave_group_request, 2>,
  request_version<metadata_request, 7>,
  request_version<find_coordinator_request, 2>,
  request_version<list_groups_request, 2>,
  request_version<create_topics_request, 4>,
  request_version<sasl_handshake_request, 1>,
  request_version<sasl_authenticate_request, 1>>;

template<typename... RequestTypes>
static constexpr auto
get_client_supported_versions_list(type_list<RequestTypes...>) {
    /// Taken from kafka/server/flex_versions.cc check there for details C++
    /// explainations
    constexpr size_t max_api_key = std::max({RequestTypes::key...});
    std::array<api_version, max_api_key + 1> versions;
    versions.fill(unlisted_api);
    ((versions[RequestTypes::key()] = RequestTypes::supported_version), ...);
    return versions;
}

/// Static mapping of the kafka requests and their respective max levels
/// supported by this kafka client
static constexpr auto g_client_mapping = get_client_supported_versions_list(
  supported_requests());

} // namespace kafka::client
