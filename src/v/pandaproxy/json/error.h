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

#include <cstdint>
#include <system_error>

namespace pandaproxy::json {

enum class error_code : uint16_t {
    // 0 is success
    invalid_json = 1,
    unable_to_serialize = 2,
};

std::error_code make_error_code(error_code);

} // namespace pandaproxy::json

namespace std {

template<>
struct is_error_code_enum<pandaproxy::json::error_code> : true_type {};

} // namespace std
