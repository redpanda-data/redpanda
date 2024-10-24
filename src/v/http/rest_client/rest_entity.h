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

#include "http/request_builder.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/beast/http/verb.hpp>

namespace http::rest_client {

// A rest entity represents an API endpoint and provides methods for CRUD
// operations on it. For example given a REST resource /person, the rest entity
// person{} is expected to provide create, retrieve, update and delete
// operations relative to that REST resource. This does not however represent a
// single person object such as /person/1.
class rest_entity {
public:
    // The root url is used as the parent path for a rest entity. So given a
    // REST resource /api/v1/person, the root url should be specified as
    // /api/v1.
    explicit rest_entity(std::string_view root_url);

    // The name of the resource which is appended to the root url to generate
    // the full path. In the canonical person example, it should be "person".
    // Along with a root url of "/api/v1" this will result in calls to the path
    // "/api/v1/person"
    virtual ss::sstring resource_name() const = 0;

    using verb = boost::beast::http::verb;
    virtual verb create_verb() const { return verb::post; }
    virtual verb retrieve_verb() const { return verb::get; }
    virtual verb update_verb() const { return verb::post; }
    virtual verb delete_verb() const { return verb::delete_; }

    virtual ~rest_entity() = default;

    // Optional args which can be supplied with any request. Any optional
    // headers or query params are applied to the HTTP request
    using optional_headers
      = std::optional<absl::flat_hash_map<ss::sstring, ss::sstring>>;
    using optional_query_params
      = std::optional<absl::flat_hash_map<ss::sstring, ss::sstring>>;

    virtual request_builder create(
      optional_headers headers = std::nullopt,
      optional_query_params query_params = std::nullopt) const;
    virtual request_builder list(
      optional_headers headers = std::nullopt,
      optional_query_params query_params = std::nullopt) const;

    // The get method retrieves an object of the type of this rest entity using
    // the identifier, eg get("1") maps to call GET /api/v1/person/1. An
    // optional subpath can be supplied for cases where a sub-resource of an
    // object is required, eg /api/v1/person/1/address
    virtual request_builder get(
      ss::sstring identifier,
      ss::sstring subpath = "",
      optional_headers headers = std::nullopt,
      optional_query_params query_params = std::nullopt) const;
    virtual request_builder update(
      ss::sstring identifier,
      optional_headers headers = std::nullopt,
      optional_query_params query_params = std::nullopt) const;
    virtual request_builder delete_(
      ss::sstring identifier,
      optional_headers headers = std::nullopt,
      optional_query_params query_params = std::nullopt) const;

    ss::sstring resource_url() const;

private:
    virtual request_builder& apply_optionals(
      request_builder& rb,
      optional_headers headers,
      optional_query_params query_params) const;

    ss::sstring _root_url;
};

} // namespace http::rest_client
