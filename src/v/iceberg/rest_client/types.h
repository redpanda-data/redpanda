/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <ada/expected.h>
#include <boost/beast/http/status.hpp>

namespace iceberg::rest_client {

// An http call related error, either a status code or a string describing an
// exception caught during the call.
using http_call_error = std::variant<boost::beast::http::status, ss::sstring>;

struct json_parse_error {
    ss::sstring context;
    ss::sstring error;
};

// The domain error represents the sum of all error types which can be
// encountered during iceberg/rest-client operations. It is a more descriptive
// alternative to std::error_code.
using domain_error = std::variant<ss::sstring, json_parse_error>;

// The core result type used by all operations in the iceberg/rest-client which
// can fail. Allows chaining of operations together and short-circuiting when an
// earlier operation in the chain has failed.
template<typename T>
using expected = tl::expected<T, domain_error>;

// Oauth token returned by the catalog server, in exchange for credentials
struct oauth_token {
    ss::sstring token;
    ss::lowres_clock::duration expiry;
    ss::lowres_clock::time_point expires_at;
};

// Static credentials expected to be supplied by redpanda when expecting an
// oauth token
struct credentials {
    ss::sstring client_id;
    ss::sstring client_secret;
};

} // namespace iceberg::rest_client
