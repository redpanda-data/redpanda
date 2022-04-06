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

#include "pandaproxy/parsing/error.h"

#include <fmt/format.h>

#include <stdexcept>
#include <string>
#include <system_error>

namespace pandaproxy::parse {

struct exception_base : std::exception {
    explicit exception_base(std::error_condition err)
      : std::exception{}
      , error{err}
      , msg{err.message()} {}
    exception_base(std::error_condition err, std::string_view msg)
      : std::exception{}
      , error{err}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::error_condition error;
    std::string msg;
};

class error final : public exception_base {
public:
    explicit error(error_code ec)
      : exception_base(make_error_code(ec).default_error_condition()) {}
    error(error_code ec, std::string_view msg)
      : exception_base(make_error_code(ec).default_error_condition(), msg) {}
};

} // namespace pandaproxy::parse
