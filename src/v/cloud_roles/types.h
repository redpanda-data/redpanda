/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <unordered_set>

namespace cloud_roles {

static constexpr auto retryable_system_error_codes = std::to_array(
  {ECONNREFUSED, ENETUNREACH, ETIMEDOUT, ECONNRESET, EPIPE});

bool is_retryable(const std::system_error& ec);

static constexpr auto retryable_http_status = std::to_array({
  boost::beast::http::status::request_timeout,
  boost::beast::http::status::gateway_timeout,
  boost::beast::http::status::bad_gateway,
  boost::beast::http::status::service_unavailable,
  boost::beast::http::status::internal_server_error,
  boost::beast::http::status::network_connect_timeout_error,
});

bool is_retryable(boost::beast::http::status status);

enum class api_request_error_kind { failed_abort, failed_retryable };

std::ostream& operator<<(std::ostream& os, api_request_error_kind kind);

struct api_request_error {
    ss::sstring reason;
    api_request_error_kind error_kind;
};

api_request_error make_abort_error(const std::exception& ex);
api_request_error make_abort_error(ss::sstring reason);

api_request_error make_retryable_error(const std::exception& ex);
api_request_error make_retryable_error(ss::sstring reason);

std::ostream&
operator<<(std::ostream& os, const api_request_error& request_error);

using api_response = std::variant<iobuf, api_request_error>;

struct malformed_api_response_error {
    std::vector<ss::sstring> missing_fields;
};

std::ostream&
operator<<(std::ostream& os, const malformed_api_response_error& err);

struct api_response_parse_error {
    ss::sstring reason;
};

std::ostream& operator<<(std::ostream& os, const api_response_parse_error& err);

using oauth_token_str = named_type<ss::sstring, struct oauth_token_str_tag>;

struct gcp_credentials {
    oauth_token_str oauth_token;
};

std::ostream& operator<<(std::ostream& os, const gcp_credentials& gc);

using aws_region_name = named_type<ss::sstring, struct s3_aws_region_name>;
using public_key_str = named_type<ss::sstring, struct s3_public_key_str>;
using private_key_str = named_type<ss::sstring, struct s3_private_key_str>;
using timestamp = std::chrono::time_point<std::chrono::system_clock>;
using s3_session_token = named_type<ss::sstring, struct s3_session_token_str>;
using storage_account = named_type<ss::sstring, struct storage_account_tag>;

struct aws_credentials {
    public_key_str access_key_id;
    private_key_str secret_access_key;
    std::optional<s3_session_token> session_token;
    aws_region_name region;
};

struct abs_credentials {
    storage_account storage_account;
    private_key_str shared_key;
};

std::ostream& operator<<(std::ostream& os, const aws_credentials& ac);

using credentials
  = std::variant<aws_credentials, gcp_credentials, abs_credentials>;

std::ostream& operator<<(std::ostream& os, const credentials& c);

using api_response_parse_result = std::variant<
  malformed_api_response_error,
  api_response_parse_error,
  api_request_error,
  credentials>;

using credentials_update_cb_t
  = ss::noncopyable_function<ss::future<>(credentials)>;

} // namespace cloud_roles
