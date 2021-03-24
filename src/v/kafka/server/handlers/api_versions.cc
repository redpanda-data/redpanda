// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/handlers.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

namespace kafka {

std::ostream& operator<<(std::ostream& os, const api_versions_response& r) {
    os << r.data;
    return os;
}

template<typename... Ts>
struct type_list {};

template<typename... Requests>
CONCEPT(requires(KafkaApiHandler<Requests>, ...))
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  produce_handler,
  fetch_handler,
  list_offsets_handler,
  metadata_handler,
  offset_fetch_handler,
  find_coordinator_handler,
  list_groups_handler,
  api_versions_handler,
  join_group_handler,
  heartbeat_handler,
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
  end_txn_handler>;

template<typename RequestType>
static auto make_api() {
    return api_versions_response_key{
      RequestType::api::key,
      RequestType::min_supported,
      RequestType::max_supported};
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

api_versions_response api_versions_handler::handle_raw(request_context& ctx) {
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
    return r;
}

ss::future<response_ptr>
api_versions_handler::handle(request_context ctx, ss::smp_service_group) {
    auto response = handle_raw(ctx);
    return ctx.respond(std::move(response));
}

} // namespace kafka
