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
#include "thirdparty/ada/ada.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/beast/http/message.hpp>

namespace http {

// Builds a request using the builder pattern. Allows setting the host, target
// (path), method, headers and query params.
class request_builder {
public:
    using error_type = ss::sstring;
    static constexpr auto default_state{"host not set"};

    using expected
      = tl::expected<boost::beast::http::request_header<>, error_type>;

    // The host supplied here is parsed and stored as a result. When the request
    // is finally built this is added as a host header. If the parse failed then
    // build returns an error variant.
    request_builder& host(std::string_view host);

    // Sets the path for the request
    request_builder& path(std::string_view path);

    request_builder& method(boost::beast::http::verb verb);

    // The following query param helpers add either k=v style params or simple
    // key based params with no values.
    request_builder&
    query_param_kv(std::string_view key, std::string_view value);

    request_builder& query_param(std::string_view q);

    request_builder&
    query_params_kv(absl::flat_hash_map<ss::sstring, ss::sstring> query_params);

    request_builder&
    query_params(absl::flat_hash_set<ss::sstring> query_params);

    // The following helpers add a header to the request. The headers are
    // overwritten, adding multiple values against the same header is not
    // supported yet.
    request_builder& header(std::string_view key, std::string_view value);

    request_builder&
    headers(absl::flat_hash_map<ss::sstring, ss::sstring> headers);

    request_builder& with_content_length(size_t content_length);

    // Adds a Bearer auth token header
    request_builder& with_bearer_auth(std::string_view token);

    request_builder& with_content_type(std::string_view content_type);

    // Builds a final HTTP request, the returned type is an error variant if the
    // host parsing failed.
    expected build();

private:
    ada::result<ada::url_aggregator> _url{
      tl::unexpected{ada::errors::generic_error}};
    std::optional<ss::sstring> _target{std::nullopt};
    boost::beast::http::request_header<> _request;
    absl::flat_hash_map<ss::sstring, ss::sstring> _query_params_kv;
    absl::flat_hash_set<ss::sstring> _query_params;
    std::optional<error_type> _error{default_state};
};

} // namespace http
