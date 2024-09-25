/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <ada/expected.h>
#include <boost/beast/http/status.hpp>

namespace iceberg::rest_client {

// An http call related error, either a status code or a string describing an
// exception caught during the call.
using http_call_error = std::variant<boost::beast::http::status, ss::sstring>;

using parse_error_msg = named_type<ss::sstring, struct parse_error_msg_t>;

struct json_parse_error {
    ss::sstring context;
    parse_error_msg error;
};

// The domain error represents the sum of all error types which can be
// encountered during rest-client operations.
using domain_error = std::variant<json_parse_error>;

// The core result type used by all operations in the iceberg/rest-client which
// can fail. Allows chaining of operations together and short-circuiting when an
// earlier operation in the chain has failed.
template<typename T>
using expected = tl::expected<T, domain_error>;

// Oauth token returned by the catalog server, in exchange for credentials
struct oauth_token {
    ss::sstring token;
    ss::lowres_clock::time_point expires_at;
};

// Static credentials expected to be supplied by redpanda when requesting an
// oauth token
struct credentials {
    ss::sstring client_id;
    ss::sstring client_secret;
};

} // namespace iceberg::rest_client
