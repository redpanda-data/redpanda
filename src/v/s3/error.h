#pragma once

#include "bytes/iobuf.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <exception>
#include <system_error>

namespace s3 {

/// Internal s3 client error codes
enum class s3_error_codes : int {
    invalid_uri,
    invalid_uri_params,
    not_enough_arguments,
};

std::error_code make_error_code(s3_error_codes ec) noexcept;

/// Error received in a response from the server
class rest_error_response : std::exception {
public:
    rest_error_response(
      std::string_view code,
      std::string_view message,
      std::string_view request_id,
      std::string_view resource);

    const char* what() const noexcept override;

    std::string_view code() const noexcept;
    std::string_view message() const noexcept;
    std::string_view request_id() const noexcept;
    std::string_view resource() const noexcept;

private:
    ss::sstring _code;
    ss::sstring _message;
    ss::sstring _request_id;
    ss::sstring _resource;
};

} // namespace s3
