/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "s3/error.h"

namespace s3 {

struct s3_error_category final : std::error_category {
    const char* name() const noexcept final { return "s3"; }
    std::string message(int ec) const final {
        switch (static_cast<s3_error_codes>(ec)) {
        case s3_error_codes::invalid_uri:
            return "Target URI shouldn't be empty or include domain name";
        case s3_error_codes::invalid_uri_params:
            return "Target URI contains invalid query parameters";
        case s3_error_codes::not_enough_arguments:
            return "Can't make request, not enough arguments";
        }
        return "unknown";
    }
};

std::error_code make_error_code(s3_error_codes ec) noexcept {
    static s3_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

rest_error_response::rest_error_response(
  std::string_view code,
  std::string_view message,
  std::string_view request_id,
  std::string_view resource)
  : _code(code)
  , _message(message)
  , _request_id(request_id)
  , _resource(resource) {}

const char* rest_error_response::what() const noexcept {
    return _message.c_str();
}
std::string_view rest_error_response::code() const noexcept { return _code; }
std::string_view rest_error_response::message() const noexcept {
    return _message;
}
std::string_view rest_error_response::request_id() const noexcept {
    return _request_id;
}
std::string_view rest_error_response::resource() const noexcept {
    return _resource;
}

} // namespace s3
