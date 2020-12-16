// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/api_versions_request.h"

#include "kafka/requests/alter_configs_request.h"
#include "kafka/requests/create_topics_request.h"
#include "kafka/requests/delete_topics_request.h"
#include "kafka/requests/describe_configs_request.h"
#include "kafka/requests/describe_groups_request.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/requests/find_coordinator_request.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/incremental_alter_configs_request.h"
#include "kafka/requests/init_producer_id_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/list_groups_request.h"
#include "kafka/requests/list_offsets_request.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/offset_commit_request.h"
#include "kafka/requests/offset_fetch_request.h"
#include "kafka/requests/produce_request.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/requests.h"
#include "kafka/requests/response.h"
#include "kafka/requests/response_writer.h"
#include "kafka/requests/sasl_authenticate_request.h"
#include "kafka/requests/sasl_handshake_request.h"
#include "kafka/requests/sync_group_request.h"

namespace kafka {

std::ostream& operator<<(std::ostream& os, const api_versions_response& r) {
    os << r.data;
    return os;
}

template<typename... Ts>
struct type_list {};

template<typename... Requests>
CONCEPT(requires(KafkaRequest<Requests>, ...))
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  produce_api,
  fetch_api,
  list_offsets_api,
  metadata_api,
  offset_fetch_api,
  find_coordinator_api,
  list_groups_api,
  api_versions_api,
  join_group_api,
  heartbeat_api,
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
  incremental_alter_configs_api>;

template<typename RequestType>
static auto make_api() {
    return api_versions_response_key{
      RequestType::key, RequestType::min_supported, RequestType::max_supported};
}

template<typename... RequestTypes>
static std::vector<api_versions_response_key>
serialize_apis(type_list<RequestTypes...>) {
    std::vector<api_versions_response_key> apis;
    (apis.push_back(make_api<RequestTypes>()), ...);
    return apis;
}

std::vector<api_versions_response_key> get_supported_apis() {
    return serialize_apis(request_types{});
}

ss::future<response_ptr>
api_versions_api::process(request_context&& ctx, ss::smp_service_group) {
    // Unlike other request types, we handle ApiVersion requests
    // with higher versions than supported. We treat such a request
    // as if it were v0 and return a response using the v0 response
    // schema. The reason for this is that the client does not yet know what
    // versions a server supports when this request is sent, so instead of
    // assuming the lowest supported version, it can use the most recent
    // version and only fallback to the old version when necessary.
    api_versions_response r;
    if (ctx.header().version > max_supported) {
        r.data.error_code = error_code::unsupported_version;
    } else {
        api_versions_request request;
        request.decode(ctx.reader(), ctx.header().version);
        r.data.error_code = error_code::none;
    }

    if (ctx.header().version > api_version(1)) {
        r.data.throttle_time_ms = std::chrono::milliseconds(
          ctx.throttle_delay_ms());
    }

    if (
      r.data.error_code == error_code::none
      || r.data.error_code == error_code::unsupported_version) {
        r.data.api_keys = get_supported_apis();
    }

    return ctx.respond(std::move(r));
}

} // namespace kafka
