/*
 * Copyright 2021 Vectorized, Inc.
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

namespace pandaproxy::parse {

struct exception_base : std::exception {
    exception_base(error_code err, std::string_view msg)
      : std::exception{}
      , error{err}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    error_code error;
    std::string msg;
};

class error final : public exception_base {
public:
    explicit error(error_code ec, std::string_view msg)
      : exception_base(ec, msg) {}
};

} // namespace pandaproxy::parse
