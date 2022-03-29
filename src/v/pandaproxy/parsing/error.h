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

namespace pandaproxy::parse {

enum class error_code : uint16_t {
    // ok = 0
    empty_param = 100,
    invalid_param = 101,
    not_acceptable = 406,
    unsupported_media_type = 415,
};

std::error_code make_error_code(error_code);

} // namespace pandaproxy::parse

namespace std {

template<>
struct is_error_code_enum<pandaproxy::parse::error_code> : true_type {};

} // namespace std
