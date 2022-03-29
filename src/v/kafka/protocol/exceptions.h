/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <fmt/core.h>

#include <exception>

namespace kafka {

struct exception_base : std::exception {
    explicit exception_base(error_code err, std::string msg)
      : std::exception{}
      , error{err}
      , msg{std::move(msg)} {}
    const char* what() const noexcept final { return msg.c_str(); }
    error_code error;
    std::string msg;
};

struct exception final : exception_base {
    explicit exception(error_code err, std::string msg)
      : exception_base{err, std::move(msg)} {}
};

} // namespace kafka
