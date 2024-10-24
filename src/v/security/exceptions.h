/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "security/errc.h"

#include <fmt/core.h>

#include <stdexcept>
#include <string_view>

namespace security {

struct exception final : public std::runtime_error {
public:
    explicit exception(std::error_code e, std::string_view msg)
      : std::runtime_error(fmt::format("{}: {}", e.message(), msg)) {}
};

} // namespace security
