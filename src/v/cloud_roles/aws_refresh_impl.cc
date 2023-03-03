/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/aws_refresh_impl.h"

#include "cloud_roles/logger.h"
#include "cloud_roles/request_response_helpers.h"

namespace cloud_roles {

struct ec2_response_schema {
    static constexpr std::string_view expiry = "Expiration";
    static constexpr std::string_view access_key_id = "AccessKeyId";
    static constexpr std::string_view secret_access_key = "SecretAccessKey";
    static constexpr std::string_view session_token = "Token";
};

aws_refresh_impl::aws_refresh_impl(
  net::unresolved_address address,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : refresh_credentials::impl(
    std::move(address), std::move(region), as, retry_params) {}

ss::future<api_response> aws_refresh_impl::fetch_credentials() {
    if (unlikely(!_role)) {
        vlog(clrl_log.info, "initializing role name");
        auto response = co_await fetch_role_name();
        // error while fetching the role, make caller handle it
        if (std::holds_alternative<api_request_error>(response)) {
            co_return response;
        }

        iobuf_const_parser parser(std::get<iobuf>(response));
        _role.emplace(parser.read_string(parser.bytes_left()));

        vlog(clrl_log.info, "fetched iam role name [{}]", *_role);
        if (_role->empty()) {
            // TODO (abhijat) create a new error kind for bad system state
            co_return api_request_error{
              .reason = "empty role name set on instance",
              .error_kind = api_request_error_kind::failed_abort};
        }
    }

    if (_role->empty()) {
        vlog(
          clrl_log.error,
          "IAM role name not populated, cannot fetch credentials");
        // TODO (abhijat) create a new error kind for bad system state
        co_return api_request_error{
          .reason = "missing IAM role name",
          .error_kind = api_request_error_kind::failed_retryable};
    }

    http::client::request_header creds_req;
    auto host = address().host();
    creds_req.insert(
      boost::beast::http::field::host, {host.data(), host.size()});
    creds_req.method(boost::beast::http::verb::get);
    creds_req.target(
      fmt::format("/latest/meta-data/iam/security-credentials/{}", *_role));
    co_return co_await make_request(
      co_await make_api_client(), std::move(creds_req));
}

api_response_parse_result aws_refresh_impl::parse_response(iobuf resp) {
    auto doc = parse_json_response(std::move(resp));
    std::vector<ss::sstring> missing_fields;
    for (const auto& key :
         {ec2_response_schema::expiry,
          ec2_response_schema::access_key_id,
          ec2_response_schema::secret_access_key,
          ec2_response_schema::session_token}) {
        if (!doc.HasMember(key.data())) {
            missing_fields.emplace_back(key);
        }
    }

    if (!missing_fields.empty()) {
        return malformed_api_response_error{
          .missing_fields = std::move(missing_fields)};
    }

    auto expiration = doc[ec2_response_schema::expiry.data()].GetString();
    next_sleep_duration(calculate_sleep_duration(parse_timestamp(expiration)));

    return aws_credentials{
      .access_key_id
      = public_key_str{doc[ec2_response_schema::access_key_id.data()]
                         .GetString()},
      .secret_access_key
      = private_key_str{doc[ec2_response_schema::secret_access_key.data()]
                          .GetString()},
      .session_token
      = s3_session_token{doc[ec2_response_schema::session_token.data()]
                           .GetString()},
      .region = region(),
    };
}

ss::future<api_response> aws_refresh_impl::fetch_role_name() {
    http::client::request_header role_req;
    auto host = address().host();
    role_req.insert(
      boost::beast::http::field::host, {host.data(), host.size()});
    role_req.method(boost::beast::http::verb::get);
    role_req.target("/latest/meta-data/iam/security-credentials/");
    co_return co_await make_request(
      co_await make_api_client(), std::move(role_req));
}

std::ostream& aws_refresh_impl::print(std::ostream& os) const {
    fmt::print(os, "aws_refresh_impl{{address:{}}}", address());
    return os;
}

} // namespace cloud_roles
