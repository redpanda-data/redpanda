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

#include <boost/property_tree/ptree.hpp>

namespace cloud_storage_clients::util {

/// \brief; Handle all client errors which are not error responses from the
/// cloud provider (e.g. connection error).
/// \param current_exception is the current exception thrown by the client
/// \param logger is the logger to use
error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, ss::logger& logger);

/// \brief: Drain the reponse stream pointed to by the 'resp' handle into an
/// iobuf
ss::future<iobuf> drain_response_stream(http::client::response_stream_ref resp);

/// \brief: Drain the reponse stream pointed to by the 'resp' handle into an
/// iobuf
ss::future<iobuf>
drain_chunked_response_stream(http::client::response_stream_ref resp);

/// \brief: Convert iobuf that contains xml data to boost::property_tree
boost::property_tree::ptree iobuf_to_ptree(iobuf&& buf, ss::logger& logger);

/// \brief: Parse timestamp in format that S3 and ABS use
std::chrono::system_clock::time_point parse_timestamp(std::string_view sv);

void log_buffer_with_rate_limiting(
  const char* msg, iobuf& buf, ss::logger& logger);

bool has_abort_or_gate_close_exception(const ss::nested_exception& ex);

/// \brief: Given a file system like path, generate the full list
/// of valid prefix paths. For instance, if the input is: a/b/log.txt,
/// return a, a/b, a/b/log.txt
std::vector<object_key> all_paths_to_file(const object_key& path);

} // namespace cloud_storage_clients::util
