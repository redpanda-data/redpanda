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

#include "kafka/protocol/errors.h"
#include "pandaproxy/error.h"
#include "pandaproxy/json/error.h"
#include "pandaproxy/json/requests/error_reply.h"

#include <fmt/format.h>

#include <stdexcept>
#include <string>

namespace pandaproxy::json {

struct exception_base : std::exception {
    exception_base(std::error_code err, std::string_view msg)
      : std::exception{}
      , error{err}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::error_code error;
    std::string msg;
};

class parse_error final : public exception_base {
public:
    explicit parse_error(size_t offset)
      : exception_base(
          make_error_code(error_code::invalid_json),
          fmt::format("parse error at offset {}", offset)) {}
};

class serialize_error final : public exception_base {
public:
    explicit serialize_error(std::error_code ec, std::string_view msg)
      : exception_base(ec, msg) {}
    explicit serialize_error(kafka::error_code ec)
      : exception_base(make_error_code(ec), kafka::error_code_to_str(ec)) {}
};

} // namespace pandaproxy::json
