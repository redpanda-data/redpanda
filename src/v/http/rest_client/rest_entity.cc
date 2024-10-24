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

#include "http/rest_client/rest_entity.h"

namespace http::rest_client {

rest_entity::rest_entity(std::string_view root_url)
  : _root_url{
      root_url.ends_with("/") ? root_url : fmt::format("{}/", root_url)} {}

request_builder rest_entity::create(
  optional_headers headers, optional_query_params query_params) const {
    request_builder rb;
    return apply_optionals(rb, headers, query_params)
      .path(resource_url())
      .method(create_verb());
}

request_builder rest_entity::list(
  optional_headers headers, optional_query_params query_params) const {
    request_builder rb;
    return apply_optionals(rb, headers, query_params)
      .path(resource_url())
      .method(retrieve_verb());
}

request_builder rest_entity::get(
  ss::sstring identifier,
  ss::sstring subpath,
  optional_headers headers,
  optional_query_params query_params) const {
    auto target = fmt::format("{}/{}", resource_url(), identifier);
    if (!subpath.empty()) {
        target = fmt::format("{}/{}", target, subpath);
    }
    request_builder rb;
    return apply_optionals(rb, headers, query_params)
      .path(target)
      .method(retrieve_verb());
}

request_builder rest_entity::update(
  ss::sstring identifier,
  optional_headers headers,
  optional_query_params query_params) const {
    request_builder rb;
    return apply_optionals(rb, headers, query_params)
      .path(fmt::format("{}/{}", resource_url(), identifier))
      .method(update_verb());
}

request_builder rest_entity::delete_(
  ss::sstring identifier,
  optional_headers headers,
  optional_query_params query_params) const {
    request_builder rb;
    return apply_optionals(rb, headers, query_params)
      .path(fmt::format("{}/{}", resource_url(), identifier))
      .method(delete_verb());
}

request_builder& rest_entity::apply_optionals(
  request_builder& rb,
  optional_headers headers,
  optional_query_params query_params) const {
    if (headers.has_value()) {
        rb.headers(headers.value());
    }

    if (query_params.has_value()) {
        rb.query_params_kv(query_params.value());
    }

    return rb;
}

ss::sstring rest_entity::resource_url() const {
    return fmt::format("{}{}", _root_url, resource_name());
}

} // namespace http::rest_client
