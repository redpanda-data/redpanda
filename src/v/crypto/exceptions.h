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

#include <stdexcept>
#include <string>

namespace crypto {
/// Base exception for all cryptographic errors
class exception : public std::runtime_error {
public:
    explicit exception(const std::string& msg)
      : std::runtime_error{msg} {}
};
} // namespace crypto
