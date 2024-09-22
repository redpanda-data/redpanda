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

#include "bytes/iobuf.h"
#include "http/client.h"
#include "http/request_builder.h"
#include "iceberg/rest_client/retry_policy.h"
#include "iceberg/rest_client/types.h"
#include "json/document.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace iceberg::rest_client {

expected<json::Document> parse_json(iobuf&& raw_response);

// The catalog client enables high level operations against a catalog rest api.
// It also ensures that a token is available before making calls.
class catalog_client {
public:
    catalog_client(
      http::abstract_client& client,
      std::string_view endpoint,
      credentials credentials,
      std::optional<std::string_view> base_url = std::nullopt,
      std::optional<std::string_view> prefix = std::nullopt,
      std::string_view api_version = "v1",
      std::unique_ptr<retry_policy> r = nullptr);

    // Acquires token from catalog API by exchanging credentials
    ss::future<> acquire_token(retry_chain_node& rtc);

    ss::future<> list_namespaces(retry_chain_node& rtc);

    ss::future<> list_namespace_tables(
      std::initializer_list<ss::sstring> namespace_parts,
      retry_chain_node& rtc);

    ss::future<> get_table(
      std::initializer_list<ss::sstring> namespace_parts,
      ss::sstring table_name,
      retry_chain_node& rtc);

    ss::future<> commit_table_updates(
      std::initializer_list<ss::sstring> namespace_parts,
      ss::sstring table_name,
      retry_chain_node& rtc);

    ss::sstring token() const;

    // The root url calculated from base url, prefix and api version. Given a
    // base url of "/b", an api version "v2" and a prefix of "x/y", the root url
    // is "/b/v2/x/y/". The root url is prepended before rest entities used when
    // making calls to the catalog service
    ss::sstring root_url() const;

    ss::future<> f();

private:
    // Acquires token if current token is unset (default state) or expired
    ss::future<> ensure_token_is_valid(retry_chain_node& rtc);

    // Builds the request from supplied builder after validating it, performs
    // the request with optional payload, and takes care of retrying according
    // to policy
    ss::future<expected<iobuf>> perform_request(
      retry_chain_node& rtc,
      http::request_builder request_builder,
      std::optional<iobuf> payload = std::nullopt);

    std::reference_wrapper<http::abstract_client> _client;
    ss::sstring _endpoint;
    credentials _credentials;
    ss::sstring _base_url;
    std::optional<ss::sstring> _prefix;
    ss::sstring _api_version;
    std::optional<oauth_token> _oauth_token{std::nullopt};
    std::unique_ptr<retry_policy> _retry_policy;
};

} // namespace iceberg::rest_client
