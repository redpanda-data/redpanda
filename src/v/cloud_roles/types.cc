/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/types.h"

#include <seastar/util/variant_utils.hh>

namespace cloud_roles {

std::ostream& operator<<(std::ostream& os, api_request_error_kind kind) {
    switch (kind) {
    case api_request_error_kind::failed_abort:
        return os << "failed_abort";
    case api_request_error_kind::failed_retryable:
        return os << "failed_retryable";
    }
}

std::ostream&
operator<<(std::ostream& os, const api_request_error& request_error) {
    fmt::print(
      os,
      "api_request_error{{reason:{}, error_kind:{}}}",
      request_error.reason,
      request_error.error_kind);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const malformed_api_response_error& err) {
    fmt::print(
      os,
      "malformed_api_response_error{{missing_fields:{}}}",
      err.missing_fields);
    return os;
}

std::ostream& operator<<(std::ostream& os, const gcp_credentials& gc) {
    fmt::print(
      os, "gcp_credentials{{oauth_token:**{}**}}", gc.oauth_token().size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const aws_credentials& ac) {
    fmt::print(
      os,
      "aws_credentials{{access_key_id: **{}**, secret_access_key: **{}**, "
      "session_token: **{}**}}",
      ac.access_key_id().size(),
      ac.secret_access_key().size(),
      ac.session_token.value_or(s3_session_token{})().size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const credentials& c) {
    ss::visit(c, [&os](auto creds) { os << creds; });
    return os;
}

std::ostream&
operator<<(std::ostream& os, const api_response_parse_error& err) {
    fmt::print(os, "api_response_parse_error{{reason:{}}}", err.reason);
    return os;
}

bool is_retryable(const std::system_error& ec) {
    auto code = ec.code();
    return std::find(
             retryable_system_error_codes.begin(),
             retryable_system_error_codes.end(),
             code.value())
           != retryable_system_error_codes.end();
}

bool is_retryable(boost::beast::http::status status) {
    return std::find(
             retryable_http_status.begin(), retryable_http_status.end(), status)
           != retryable_http_status.end();
}

api_request_error make_abort_error(const std::exception& ex) {
    return api_request_error{
      .reason = ex.what(),
      .error_kind = api_request_error_kind::failed_abort,
    };
}

api_request_error make_abort_error(ss::sstring reason) {
    return api_request_error{
      .reason = reason, .error_kind = api_request_error_kind::failed_abort};
}

api_request_error make_retryable_error(const std::exception& ex) {
    return api_request_error{
      .reason = ex.what(),
      .error_kind = api_request_error_kind::failed_retryable,
    };
}

api_request_error make_retryable_error(ss::sstring reason) {
    return api_request_error{
      .reason = reason, .error_kind = api_request_error_kind::failed_retryable};
}

} // namespace cloud_roles
