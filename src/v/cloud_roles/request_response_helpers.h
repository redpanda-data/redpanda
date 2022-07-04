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

#include "cloud_roles/types.h"
#include "http/client.h"
#include "json/document.h"

namespace cloud_roles {

inline constexpr std::chrono::milliseconds default_request_timeout{5000};

ss::future<api_response> make_request(
  http::client client,
  http::client::request_header req,
  std::chrono::milliseconds timeout = default_request_timeout);

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  iobuf content,
  std::chrono::milliseconds timeout = default_request_timeout);

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  ss::sstring content,
  std::chrono::milliseconds timeout = default_request_timeout);

ss::future<boost::beast::http::status>
get_status(http::client::response_stream_ref& resp);

json::Document parse_json_response(iobuf resp);

} // namespace cloud_roles
