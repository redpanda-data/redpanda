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

#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>

#include <exception>

namespace cloud_storage_clients {

/// \brief Azure Blob Storage error codes
enum class abs_error_code {
    _unknown,
    blob_not_found,
    authentication_failed,
    server_busy,
    internal_error,
    operation_timed_out,
    system_in_use,
    account_being_created,
    resource_already_exists,
    blob_already_exists,
    invalid_blob,
    pending_copy_operation,
    snapshot_present,
    blob_being_rehydrated,
    container_being_disabled,
    container_being_deleted,
    container_not_found
};

/// Operators to use with lexical_cast
std::istream& operator>>(std::istream& i, abs_error_code& code);

/// Error received in a response from the server
class abs_rest_error_response : std::exception {
public:
    abs_rest_error_response(
      ss::sstring code,
      ss::sstring message,
      boost::beast::http::status http_code);

    const char* what() const noexcept override;

    abs_error_code code() const noexcept;
    std::string_view code_string() const noexcept;
    std::string_view message() const noexcept;
    boost::beast::http::status http_code() const noexcept;

private:
    abs_error_code _code;
    /// Error code string representation, this string is almost always short
    /// enough for short string optimisation.
    ss::sstring _code_str;
    ss::sstring _message;
    boost::beast::http::status _http_code;
};

} // namespace cloud_storage_clients
