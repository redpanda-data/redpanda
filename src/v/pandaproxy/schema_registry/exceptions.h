/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <system_error>

namespace pandaproxy::schema_registry {

// Similar to std::system_error but with more control over the message
class exception_base : public std::exception {
public:
    explicit exception_base(std::error_code ec)
      : std::exception{}
      , _ec{ec}
      , _msg{ec.message()} {}
    exception_base(std::error_code ec, std::string msg)
      : std::exception{}
      , _ec{ec}
      , _msg{std::move(msg)} {}

    const char* what() const noexcept final { return _msg.c_str(); }
    const std::string& message() const noexcept { return _msg; }
    const std::error_code& code() const noexcept { return _ec; }

private:
    std::error_code _ec;
    std::string _msg;
};

class exception final : public exception_base {
public:
    explicit exception(std::error_code ec)
      : exception_base{ec} {}
    exception(std::error_code ec, std::string msg)
      : exception_base{ec, std::move(msg)} {}
};

} // namespace pandaproxy::schema_registry
