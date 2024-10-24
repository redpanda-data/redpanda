/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "apply_abs_oauth_credentials.h"

#include "base/vlog.h"

namespace cloud_roles {
apply_abs_oauth_credentials::apply_abs_oauth_credentials(
  const abs_oauth_credentials& credentials)
  : _oauth_token{fmt::format("Bearer {}", credentials.oauth_token())}
  , _timesource{} {}

std::error_code apply_abs_oauth_credentials::add_auth(
  http::client::request_header& header) const {
    auto token = _oauth_token();
    // x-ms-version requests a specific version for the abs api, the hardcoded
    // value is the one we test
    header.set("x-ms-version", azure_storage_api_version);
    auto iso_ts = _timesource.format_http_datetime();
    header.set("x-ms-date", {iso_ts.data(), iso_ts.size()});
    header.insert(
      boost::beast::http::field::authorization, {token.data(), token.size()});
    return {};
}

void apply_abs_oauth_credentials::reset_creds(credentials creds) {
    if (!std::holds_alternative<abs_oauth_credentials>(creds)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "credential applier reset with incorrect credential type {}",
          creds));
    }
    *this = apply_abs_oauth_credentials{std::get<abs_oauth_credentials>(creds)};
}

std::ostream& apply_abs_oauth_credentials::print(std::ostream& os) const {
    fmt::print(os, "apply_abs_oauth_credentials");
    return os;
}

} // namespace cloud_roles
