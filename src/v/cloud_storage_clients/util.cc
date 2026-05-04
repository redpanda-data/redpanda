/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/util.h"

#include "base/vlog.h"
#include "http/utils.h"
#include "net/connection.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

#include <exception>
#include <system_error>

namespace {

bool is_abort_or_gate_close_exception(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::abort_requested_exception&) {
        return true;
    } catch (const ss::gate_closed_exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace cloud_storage_clients::util {

bool has_abort_or_gate_close_exception(const ss::nested_exception& ex) {
    return is_abort_or_gate_close_exception(ex.inner)
           || is_abort_or_gate_close_exception(ex.outer);
}

bool is_nested_reconnect_error(const ss::nested_exception& ex) {
    try {
        std::rethrow_exception(ex.inner);
    } catch (const std::system_error& e) {
        if (!net::is_reconnect_error(e)) {
            return false;
        }
    } catch (...) {
        return false;
    }
    try {
        std::rethrow_exception(ex.outer);
    } catch (const std::system_error& e) {
        if (!net::is_reconnect_error(e)) {
            return false;
        }
    } catch (...) {
        return false;
    }
    return true;
}

template<typename Logger>
error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, Logger& logger) {
    auto outcome = error_outcome::retry;

    try {
        std::rethrow_exception(current_exception);
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            vlog(logger.warn, "File removed during download: ", e.path1());
            outcome = error_outcome::retry;
        } else {
            vlog(logger.error, "Filesystem error {}", e);
            outcome = error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (net::is_reconnect_error(cerr)) {
            vlog(
              logger.warn,
              "System error susceptible for retry {}",
              cerr.what());

        } else {
            vlog(logger.error, "System error {}", cerr);
            outcome = error_outcome::fail;
        }
    } catch (const ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        vlog(logger.warn, "Connection timeout {}", terr.what());
    } catch (const boost::system::system_error& err) {
        if (
          err.code() != boost::beast::http::error::end_of_stream
          && err.code() != boost::beast::http::error::partial_message) {
            vlog(logger.warn, "Connection failed {}", err.what());
            outcome = error_outcome::fail;
        } else {
            // This is a short read error that can be caused by the abrupt TLS
            // shutdown. The content of the received buffer is discarded in this
            // case and http client receives an empty buffer.
            vlog(
              logger.info,
              "Server disconnected: '{}', retrying HTTP request",
              err.what());
        }
    } catch (const ss::gate_closed_exception&) {
        vlog(logger.debug, "Gate closed");
        throw;
    } catch (const ss::abort_requested_exception&) {
        vlog(logger.debug, "Abort requested");
        throw;
    } catch (const ss::nested_exception& ex) {
        if (has_abort_or_gate_close_exception(ex)) {
            vlog(logger.debug, "Nested abort or gate closed: {}", ex);
            throw;
        } else if (is_nested_reconnect_error(ex)) {
            vlog(logger.warn, "Connection error {}", std::current_exception());
        } else {
            vlog(logger.error, "Unexpected error {}", std::current_exception());
            outcome = error_outcome::fail;
        }
    } catch (...) {
        vlog(logger.error, "Unexpected error {}", std::current_exception());
        outcome = error_outcome::fail;
    }

    return outcome;
}

template error_outcome
handle_client_transport_error<ss::logger>(std::exception_ptr, ss::logger&);
template error_outcome handle_client_transport_error<retry_chain_logger>(
  std::exception_ptr, retry_chain_logger&);

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

std::vector<object_key> all_paths_to_file(const object_key& path) {
    if (!path().has_filename()) {
        return {};
    }

    std::vector<object_key> paths;
    std::filesystem::path current_path;
    for (auto path_iter = path().begin(); path_iter != path().end();
         ++path_iter) {
        if (current_path == "") {
            current_path += *path_iter;
        } else {
            current_path /= *path_iter;
        }

        paths.emplace_back(current_path);
    }

    return paths;
}

void url_encode_target(http::client::request_header& header) {
    auto query_pos = header.target().find_first_of("?");
    // encode full target as there are no query parameters
    if (query_pos == std::string::npos) {
        header.target(
          std::string(
            http::uri_encode(header.target(), http::uri_encode_slash::no)));
    } else {
        // encode only the path part of the target
        // TODO: add individual query parameters encoding here as well.
        header.target(
          fmt::format(
            "{}{}",
            http::uri_encode(
              std::string_view(header.target().begin(), query_pos),
              http::uri_encode_slash::no),
            header.target().substr(query_pos)));
    }
}

response_content_type
get_response_content_type(const http::client::response_header& headers) {
    static constexpr boost::beast::string_view content_type_name
      = "Content-Type";
    if (auto iter = headers.find(content_type_name); iter != headers.end()) {
        if (iter->value().find("json") != std::string_view::npos) {
            return response_content_type::json;
        }

        if (iter->value().find("xml") != std::string_view::npos) {
            return response_content_type::xml;
        }
    }

    return response_content_type::unknown;
}

} // namespace cloud_storage_clients::util
