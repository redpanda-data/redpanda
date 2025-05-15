/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"

#include <system_error>

namespace panda_link {
enum class errc : int {
    success = 0,
    panda_link_does_not_exist,
    panda_link_already_exists,
    invalid_configuration,
    unknown_error,
    internal_server_error,
};

std::error_code make_error_code(errc e) noexcept;

class error_info {
public:
    explicit error_info(errc ec)
      : _ec(ec)
      , _msg(make_error_code(ec).message()) {}

    error_info(errc ec, std::string msg)
      : _ec(ec)
      , _msg(std::move(msg)) {}

    const errc& code() const noexcept { return _ec; }
    const std::string& message() const noexcept { return _msg; }

private:
    errc _ec;
    std::string _msg;
};

template<typename T>
using result = result<T, error_info>;
} // namespace panda_link

namespace std {
template<>
struct is_error_code_enum<panda_link::errc> : true_type {};
} // namespace std
