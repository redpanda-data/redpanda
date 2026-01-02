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
#include "bytes/iobuf_parser.h"
#include "cloud_storage_clients/types.h"
#include "http/client.h"

#include <seastar/util/log.hh>

#include <boost/property_tree/ptree.hpp>

namespace cloud_storage_clients::util {

/// \brief; Handle all client errors which are not error responses from the
/// cloud provider (e.g. connection error).
/// \param current_exception is the current exception thrown by the client
/// \param logger is the logger to use
template<typename Logger>
error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, Logger& logger);

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

// Helper to URL encode the target field in the request header
// TODO: This should be replaced after we will represent URIs as structs
void url_encode_target(http::client::request_header& header);

response_content_type
get_response_content_type(const http::client::response_header& headers);

struct mime_header {
    static constexpr std::string_view content_type_field = "Content-Type";
    static constexpr std::string_view content_id_field = "Content-ID";

public:
    using field = boost::beast::http::field;
    using field_map_t = std::unordered_map<field, ss::sstring>;

    template<typename T>
    std::optional<T> content_id(
      const std::function<std::optional<T>(std::string_view)> f) const {
        return get(field::content_id).and_then(f);
    }
    std::optional<ss::sstring> get(field f) const;
    field_map_t::const_iterator find(field f) const { return _fields.find(f); }
    field_map_t::const_iterator end() const { return _fields.end(); }

    // TODO: should return optional or someting?
    static mime_header from(iobuf_parser& in);

private:
    field_map_t _fields;
};

struct multipart_response_parser {
    explicit multipart_response_parser(iobuf b, ss::sstring delim);
    std::optional<iobuf> get_part();

private:
    void advance_to_first_boundary();
    iobuf _buffer;
    iobuf_const_parser _parser;
    ss::sstring _delim;
    bool _found_first{false};
    bool _done{false};
};

struct multipart_subresponse {
    using status = boost::beast::http::status;
    status result() const;
    bool is_ok() const;
    std::optional<ss::sstring> error(std::string_view error_code_name) const;
    // extract error message from the response body. non-const because it
    // requires a share from the cached response body
    std::optional<ss::sstring> error(const std::function<ss::sstring(iobuf)>&);
    static multipart_subresponse from(iobuf_parser& in);

private:
    static std::vector<boost::asio::const_buffer>
    iobuf_to_constbufseq(const iobuf& buf);
    using parser_t = boost::beast::http::response_parser<http::iobuf_body>;
    using response_t = parser_t::value_type;
    std::optional<response_t> _response{};
    size_t _noctets{0};
    boost::beast::error_code _ec{};
    bool _header_done{false};
    iobuf _body;
};

} // namespace cloud_storage_clients::util
