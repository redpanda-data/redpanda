/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/gcp_refresh_impl.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_roles/logger.h"
#include "cloud_roles/request_response_helpers.h"
#include "json/document.h"
#include "json/istreamwrapper.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cloud_roles {
static constexpr std::string_view token_path
  = "/computeMetadata/v1/instance/service-accounts/default/token";

/// This header must be sent to GCP metadata API along with each request
struct metadata_flavor {
    static constexpr std::string_view header_name = "Metadata-Flavor";

    static constexpr std::string_view value = "Google";
};

struct gcp_response_schema {
    static constexpr std::string_view expiry_field = "expires_in";
    static constexpr std::string_view token_field = "access_token";
};

gcp_refresh_impl::gcp_refresh_impl(
  net::unresolved_address address,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : refresh_credentials::impl(
    std::move(address), std::move(region), as, retry_params) {}

ss::future<api_response> gcp_refresh_impl::fetch_credentials() {
    http::client::request_header oauth_req;

    oauth_req.method(boost::beast::http::verb::get);
    oauth_req.target(token_path.data());
    oauth_req.set(
      metadata_flavor::header_name.data(), metadata_flavor::value.data());

    co_return co_await make_request(
      co_await make_api_client(), std::move(oauth_req));
}

api_response_parse_result gcp_refresh_impl::parse_response(iobuf response) {
    auto doc = parse_json_response(std::move(response));
    std::vector<ss::sstring> missing_fields;
    if (!doc.HasMember(gcp_response_schema::expiry_field.data())) {
        missing_fields.emplace_back(gcp_response_schema::expiry_field);
    }

    if (!doc.HasMember(gcp_response_schema::token_field.data())) {
        missing_fields.emplace_back(gcp_response_schema::token_field);
    }

    if (!missing_fields.empty()) {
        return malformed_api_response_error{
          .missing_fields = std::move(missing_fields)};
    }

    next_sleep_duration(calculate_sleep_duration(
      doc[gcp_response_schema::expiry_field.data()].GetInt64()));
    return gcp_credentials{
      .oauth_token = oauth_token_str{
        doc[gcp_response_schema::token_field.data()].GetString()}};
}

std::ostream& gcp_refresh_impl::print(std::ostream& os) const {
    fmt::print(os, "gcp_refresh_impl{{address:{}}}", address());
    return os;
}

} // namespace cloud_roles
