/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "seastar/util/log.hh"

#include <string_view>

namespace wasm {

/**
 * A logging interface for wasm transforms.
 *
 */
class logger {
public:
    logger() = default;
    virtual ~logger() = default;
    logger(const logger&) = delete;
    logger& operator=(const logger&) = delete;
    logger(logger&&) = delete;
    logger& operator=(logger&&) = delete;

    virtual void log(ss::log_level lvl, std::string_view message) noexcept = 0;
};

} // namespace wasm
