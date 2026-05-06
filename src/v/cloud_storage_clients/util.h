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
#include "cloud_storage_clients/types.h"
#include "http/client.h"

#include <seastar/util/log.hh>

namespace cloud_storage_clients::util {

/// \brief; Handle all client errors which are not error responses from the
/// cloud provider (e.g. connection error).
/// \param current_exception is the current exception thrown by the client
/// \param logger is the logger to use
template<typename Logger>
error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, Logger& logger);

/// \brief: Parse timestamp in format that S3 and ABS use
std::chrono::system_clock::time_point parse_timestamp(std::string_view sv);

bool has_abort_or_gate_close_exception(const ss::nested_exception& ex);

/// \brief: Given a file system like path, generate the full list
/// of valid prefix paths. For instance, if the input is: a/b/log.txt,
/// return a, a/b, a/b/log.txt
std::vector<object_key> all_paths_to_file(const object_key& path);

// Helper to URL encode the target field in the request header
// TODO: This should be replaced after we will represent URIs as structs
void url_encode_target(http::client::request_header& header);

response_content_type
get_response_content_type(const http::client::response_header& headers);

} // namespace cloud_storage_clients::util
