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

ss::future<api_response> make_request(
  http::client client,
  http::client::request_header req,
  std::optional<std::chrono::milliseconds> timeout = std::nullopt);

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  iobuf content,
  std::optional<std::chrono::milliseconds> timeout = std::nullopt);

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  ss::sstring content,
  std::optional<std::chrono::milliseconds> timeout = std::nullopt);

ss::future<boost::beast::http::status>
get_status(http::client::response_stream_ref& resp);

json::Document parse_json_response(iobuf resp);

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv);

} // namespace cloud_roles
