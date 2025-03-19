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

#include "base/seastarx.h"
#include "pandaproxy/parsing/error.h"
#include "strings/utf8.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <stdexcept>
#include <string>
#include <system_error>

namespace pandaproxy::parse {

struct exception_base : std::exception {
    explicit exception_base(std::error_code err)
      : std::exception{}
      , error{err}
      , msg{err.message()} {}
    exception_base(std::error_code err, std::string_view msg)
      : std::exception{}
      , error{err}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::error_code error;
    std::string msg;
};

class error final : public exception_base {
public:
    explicit error(error_code ec)
      : exception_base(ec) {}
    error(error_code ec, std::string_view msg)
      : exception_base(ec, msg) {}
};

struct pp_parsing_error : public default_control_character_thrower {
    pp_parsing_error(
      std::string_view unsanitized_string, std::string_view context)
      : default_control_character_thrower(unsanitized_string)
      , _context(context) {}

    [[noreturn]] [[gnu::cold]] void conversion_error() override {
        throw parse::error(
          parse::error_code::invalid_param,
          fmt::format(
            "Parameter contained invalid control characters while parsing "
            "{}: {}",
            _context,
            get_sanitized_string()));
    }

private:
    std::string_view _context;
};

} // namespace pandaproxy::parse
