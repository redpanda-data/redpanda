/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "azure_vm_refresh_impl.h"

#include "json/ostreamwrapper.h"
#include "json/schema.h"
#include "request_response_helpers.h"

#include <rapidjson/error/en.h>

namespace cloud_roles {

azure_vm_refresh_impl::azure_vm_refresh_impl(
  net::unresolved_address address,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : refresh_credentials::impl(
      std::move(address), std::move(region), as, retry_params) {}

std::ostream& azure_vm_refresh_impl::print(std::ostream& os) const {
    fmt::print(os, "azure_vm_refresh_impl{{address:{}}}", address());
    return os;
}

ss::future<api_response> azure_vm_refresh_impl::fetch_credentials() {
    auto client_id_opt = config::shard_local_cfg()
                           .cloud_storage_azure_managed_identity_id.value();
    if (unlikely(!client_id_opt.has_value())) {
        // This implementation requires client_id from config to be set.
        // Strictly speaking, IMDS service does not require it if there is only
        // a system-assigned managed identity or only one user-assigned managed
        // identity but it would be brittle to not specify it, it could break if
        // the user added a new one. This verification is also performed in
        // admin/server.cc::patch_cluster_config
        vlog(
          clrl_log.error,
          "missing cloud_storage_azure_managed_identity_id in config");
        // return value is not a perfect match but it works
        co_return api_request_error{
          .status = boost::beast::http::status::bad_request,
          .reason = "missing cloud_storage_azure_managed_identity_id",
          .error_kind = api_request_error_kind::failed_abort};
    }

    // performs this GET
    // 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://storage.azure.com/&client_id={$client_id}'
    // to the vm-local IMDS
    auto req = http::client::request_header{};
    req.method(boost::beast::http::verb::get);
    // TODO fix deps and use boost::url
    req.target(fmt::format(
      "/metadata/identity/oauth2/token?"
      "api-version=2018-02-01"
      "&resource=https%3A%2F%2Fstorage.azure.com%2F"
      "&client_id={}",
      client_id_opt.value()));
    req.set("Metadata", "true");

    co_return co_await make_request(
      co_await make_api_client("azure_vm_instance_metadata"), std::move(req));
}

api_response_parse_result
azure_vm_refresh_impl::parse_response(iobuf response) {
    // Simplified schema for (an example)
    //  {
    //    "token_type": "Bearer",
    //    "expires_in": "3599",
    //    "access_token":
    //    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1uQ19WWmNBVGZNNXBP..."
    //  }
    // Note that the expires_in field is a string, not an integer,
    // so a simple regex is used to validate a positive number of up of 18
    // digits. The complete response has more fields but we just care for
    // these 3.

    constexpr static auto success_schema_str = R"json(
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "token_type": {
      "type": "string",
      "pattern": "Bearer"
    },
    "expires_in": {
      "type": "string",
      "pattern":  "^ *\\+?[0-9]{1,18} *$"
    },
    "access_token": {
      "type": "string"
    }
  },
  "required": [
    "token_type", 
    "expires_in",
    "access_token"
  ]
}
)json";

    auto maybe_jresp = parse_json_response_and_validate(
      success_schema_str, std::move(response));
    return std::visit(
      ss::make_visitor(
        [](auto err_resp) -> api_response_parse_result {
            return std::move(err_resp);
        },
        [&](json::Document jresp) -> api_response_parse_result {
            auto& expires_in_str = jresp["expires_in"];
            next_sleep_duration(
              std::chrono::seconds{boost::lexical_cast<int64_t>(
                expires_in_str.GetString(), expires_in_str.GetStringLength())});
            auto& access_token = jresp["access_token"];
            return abs_oauth_credentials{oauth_token_str{
              access_token.GetString(), access_token.GetStringLength()}};
        }),
      std::move(maybe_jresp));
}

} // namespace cloud_roles
