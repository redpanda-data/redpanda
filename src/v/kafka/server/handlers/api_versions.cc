// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/handlers/handlers.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

namespace kafka {
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

static chunked_vector<api_versions_response_key>
get_supported_apis(bool is_idempotence_enabled, bool are_transactions_enabled) {
    auto all_api = serialize_apis(request_types{});

    chunked_vector<api_versions_response_key> filtered;
    std::copy_if(
      all_api.begin(),
      all_api.end(),
      std::back_inserter(filtered),
      [is_idempotence_enabled,
       are_transactions_enabled](api_versions_response_key api) {
          if (!is_idempotence_enabled) {
              if (api.api_key == init_producer_id_handler::api::key) {
                  return false;
              }
          }
          if (!are_transactions_enabled) {
              if (api.api_key == add_partitions_to_txn_handler::api::key) {
                  return false;
              }
              if (api.api_key == txn_offset_commit_handler::api::key) {
                  return false;
              }
              if (api.api_key == add_offsets_to_txn_handler::api::key) {
                  return false;
              }
              if (api.api_key == end_txn_handler::api::key) {
                  return false;
              }
          }
          return true;
      });
    return filtered;
}

struct APIs {
    APIs() {
        base = get_supported_apis(false, false);
        idempotence = get_supported_apis(true, false);
        transactions = get_supported_apis(true, true);
    }

    chunked_vector<api_versions_response_key> base;
    chunked_vector<api_versions_response_key> idempotence;
    chunked_vector<api_versions_response_key> transactions;
};

static thread_local APIs supported_apis;

chunked_vector<api_versions_response_key> get_supported_apis() {
    return get_supported_apis(
      config::shard_local_cfg().enable_idempotence.value(),
      config::shard_local_cfg().enable_transactions.value());
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

    if (
      r.data.error_code == error_code::none
      || r.data.error_code == error_code::unsupported_version) {
        if (!ctx.is_idempotence_enabled()) {
            r.data.api_keys = supported_apis.base.copy();
        } else if (!ctx.are_transactions_enabled()) {
            r.data.api_keys = supported_apis.idempotence.copy();
        } else {
            r.data.api_keys = supported_apis.transactions.copy();
        }
    }
    return r;
}

ss::future<response_ptr>
api_versions_handler::handle(request_context ctx, ss::smp_service_group) {
    auto response = handle_raw(ctx);
    return ctx.respond(std::move(response));
}

} // namespace kafka
