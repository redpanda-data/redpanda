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

#include "bytes/iobuf.h"
#include "http/client.h"
#include "http/request_builder.h"
#include "iceberg/rest_client/retry_policy.h"
#include "iceberg/rest_client/types.h"
#include "json/document.h"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace iceberg::rest_client {

using base_path = named_type<ss::sstring, struct base_path_t>;
using prefix_path = named_type<ss::sstring, struct prefix_t>;
using api_version = named_type<ss::sstring, struct api_version_t>;

// A client source generates low level http clients for the catalog client to
// make API calls with. Once a generic http client pool is implemented, it can
// use the same interface and hand out client leases instead of unique ptrs.
struct client_source {
    // A client returned by this method call is owned by the caller. It should
    // be shut down after use by the caller.
    virtual std::unique_ptr<http::abstract_client> acquire() = 0;
    virtual ~client_source() = default;
};

// Holds parts of a root path used by catalog client
struct path_components {
    path_components(
      std::optional<base_path> base = std::nullopt,
      std::optional<prefix_path> prefix = std::nullopt,
      std::optional<api_version> api_version = std::nullopt);

    // Returns root path for use in API calls not related to oauth token, joins
    // base, api version and prefix with slashes and adds a terminating slash.
    // eg for polaris:
    // /api/catalog/v1/prefix
    ss::sstring root_path() const;
    // Same as root path but does not include the prefix, eg for polaris:
    // /api/catalog/v1/oauth/tokens
    ss::sstring token_api_path() const;

private:
    base_path _base;
    prefix_path _prefix;
    api_version _api_version;
};

// helper intended to catch JSON parse errors when parsing an iobuf to json
// document before passing the document to more specific parsers
expected<json::Document> parse_json(iobuf&& raw_response);

// The catalog client enables high level operations against a catalog rest api.
// It also ensures that a token is available before making calls. A client is
// lightweight and expected to be created for single operations or closely
// related operations.
class catalog_client {
public:
    /// \brief Construct a catalog client
    /// \param client_source returns unique ptrs to clients for making API calls
    /// \param endpoint the rest catalog server hostname to connect to,
    /// including scheme
    /// \param credentials credentials to be used to acquire oauth token from
    /// catalog server
    /// \param base_path an optional path to be prefixed to all API calls made
    /// by this client
    /// \param prefix an optional prefix to be added after the
    /// API version, applied to all calls made by this client, except for oauth
    /// token requests
    /// \param api_version api version used to construct URLs,
    /// defaults to v1
    /// \param token an optional oauth token which will be used
    /// if valid. If expired, a new one will be acquired \param retry_policy a
    /// retry policy used to determine how failing calls will be retried
    catalog_client(
      client_source& client_source,
      ss::sstring endpoint,
      credentials credentials,
      std::optional<base_path> base_path = std::nullopt,
      std::optional<prefix_path> prefix = std::nullopt,
      std::optional<api_version> api_version = std::nullopt,
      std::optional<oauth_token> token = std::nullopt,
      std::unique_ptr<retry_policy> retry_policy = nullptr);

private:
    // The root url calculated from base url, prefix and api version. Given a
    // base url of "/b", an api version "v2" and a prefix of "x/y", the root url
    // is "/b/v2/x/y/". The root url is prefixed before rest entities used when
    // making calls to the catalog service
    ss::sstring root_path() const;

    // Acquires token from catalog API by exchanging credentials
    ss::future<expected<oauth_token>> acquire_token(retry_chain_node& rtc);

    // Ensures a token is acquired if current token is unset (default state) or
    // expired. Acquired token is cached for future calls.
    ss::future<expected<ss::sstring>> ensure_token(retry_chain_node& rtc);

    // Builds the request from supplied builder after validating it, performs
    // the request with optional payload, and takes care of retrying according
    // to policy
    ss::future<expected<iobuf>> perform_request(
      retry_chain_node& rtc,
      http::request_builder request_builder,
      std::optional<iobuf> payload = std::nullopt);

    std::reference_wrapper<client_source> _client_source;
    ss::sstring _endpoint;
    credentials _credentials;
    path_components _path_components;
    std::optional<oauth_token> _oauth_token{std::nullopt};
    std::unique_ptr<retry_policy> _retry_policy;

    friend class catalog_client_tester;
};

} // namespace iceberg::rest_client
