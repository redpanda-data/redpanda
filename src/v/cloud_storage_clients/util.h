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

#include <expected>

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

/// \brief MIME header representation for multipart response parts
///
/// Represents MIME-style headers found in individual parts of a multipart
/// HTTP response. Each part in a multipart response can have its own headers
/// (Content-Type, Content-ID, etc.) that describe the part's metadata.
/// This is commonly used in batch operations where cloud storage APIs return
/// multiple subresponses in a single HTTP response.
///
/// NOTE: We need a special parser for this because those included w/
/// boost::beast expect a status line at the front of response, no exceptions.
/// In principle, we could stick the status line at the front buffer and hand
/// off to beast, but we would still need to search forward for the first blank
/// line, besides which just getting the resulting slice ready for beast would
/// probably be even more code.
struct mime_header {
    using field = boost::beast::http::field;
    using field_map_t = std::unordered_map<field, ss::sstring>;

    /// \brief Retrieve and transform the Content-ID field
    ///
    /// Retrieves the Content-ID field value and applies the provided
    /// transformation function to convert it to the desired type.
    /// This is useful for parsing structured Content-IDs that may contain
    /// object keys or other identifiers.
    ///
    /// \param f Transformation function to parse the Content-ID string
    /// \return Transformed Content-ID value, or nullopt if field is missing
    ///         or transformation fails
    template<typename T>
    std::optional<T> content_id(
      const std::function<std::optional<T>(std::string_view)> f) const {
        return get(field::content_id).and_then(f);
    }

    /// \brief Get the value of a MIME header field
    ///
    /// \param f The header field to retrieve
    /// \return The field value, or nullopt if not present
    std::optional<ss::sstring> get(field f) const;

    /// \brief Find a header field in the collection
    field_map_t::const_iterator find(field f) const { return _fields.find(f); }

    /// \brief Get iterator to end of header fields
    field_map_t::const_iterator end() const { return _fields.end(); }

    /// \brief Parse MIME headers from an iobuf
    ///
    /// Reads MIME-style headers from the parser until an empty line is
    /// encountered. Headers are expected in "Field-Name: value" format.
    ///
    /// \param in Parser positioned at the start of MIME headers
    /// \return Parsed MIME header structure
    static mime_header from(iobuf_parser& in);

private:
    field_map_t _fields;
};

/// \brief Parser for multipart HTTP response bodies
///
/// Parses HTTP responses with multipart content encoding, where multiple
/// subresponses are separated by boundary delimiters. This is used by cloud
/// storage APIs (like ABS batch delete) to return multiple operation results
/// in a single HTTP response. The parser extracts individual parts between
/// boundaries, allowing each subresponse to be processed independently.
struct multipart_response_parser {
    /// \param b The complete multipart response body
    /// \param delim The boundary delimiter string (with leading dashes)
    explicit multipart_response_parser(iobuf b, ss::sstring delim);

    /// \brief Extract the next part from the multipart response
    ///
    /// Returns the content of the next part in the multipart response,
    /// excluding the boundary markers. Call repeatedly to iterate through
    /// all parts in the response.
    ///
    /// \return The next part's content, or nullopt if all parts have been
    ///         consumed or the final boundary has been reached
    std::optional<iobuf> get_part();

private:
    void advance_to_first_boundary();
    iobuf _buffer;
    iobuf_const_parser _parser;
    ss::sstring _delim;
    bool _found_first{false};
    bool _done{false};
};

/// \brief Individual HTTP response from a multipart response body
///
/// Represents a single HTTP response extracted from a multipart response.
/// Each subresponse contains its own HTTP status code, headers, and body,
/// allowing batch operations to return individual results for each operation
/// within a single HTTP response. This is used to parse individual delete
/// results from cloud storage batch operations.
struct multipart_subresponse {
    using status = boost::beast::http::status;

    /// \brief Get the HTTP status code of this subresponse
    ///
    /// \return The HTTP status code (e.g., 200 OK, 404 Not Found)
    status result() const;

    /// \brief Check if this subresponse indicates success
    ///
    /// \return True if the status code is in the 2xx range or 404
    bool is_ok() const;

    /// \brief Extract an error message from the response headers if one is
    /// present.
    ///
    /// \param error_code_name The name of the error code field to extract
    ///        (e.g., "x-ms-error-code" in an ABS response)
    /// \return The error message, or nullopt if not found or not an error
    std::optional<ss::sstring> error(std::string_view error_code_name) const;

    /// \brief Extract an error message from the response body if one is present
    ///
    /// Note that this function is non-const because the transformation requires
    /// a share from a stored body.
    ///
    /// \param Transformation function to extract some string from the body,
    ///        (e.g. a known field from a raw json object)
    ///  \return The extracted error message, or nullopt if not an error
    std::optional<ss::sstring> error(const std::function<ss::sstring(iobuf)>&);

    /// \brief Parse a subresponse from an iobuf
    ///
    /// Parses a single HTTP response (with headers and body) from the given
    /// parser. The parser should be positioned at the start of an HTTP
    /// response within a multipart response part.
    ///
    /// \param in Parser positioned at the start of an HTTP response
    /// \return Parsed subresponse structure
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

std::expected<ss::sstring, ss::sstring>
find_multipart_boundary(const http::client::response_header&);

} // namespace cloud_storage_clients::util
