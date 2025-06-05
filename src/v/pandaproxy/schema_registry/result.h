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
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"

namespace pandaproxy::schema_registry {

/// \brief error_info stores an error_code and custom message.
///
/// This class is useful for transporting via an outcome::result
/// and automatic conversion to an `exception`.
/// See `outcome_throw_as_system_error_with_payload`.
class error_info {
public:
    error_info() = default;
    error_info(error_code ec, std::string msg)
      : _ec{ec}
      , _msg{std::move(msg)} {}

    const error_code& code() const noexcept { return _ec; }
    const std::string& message() const noexcept { return _msg; }

private:
    error_code _ec;
    std::string _msg;
};

inline exception as_exception(const error_info& ei) {
    return exception(ei.code(), ei.message());
}

///\brief Integrate error_info with outcome
inline std::error_code make_error_code(const error_info& ei) {
    return make_error_code(ei.code());
}

///\brief Integrate error_info with outcome
inline void outcome_throw_as_system_error_with_payload(const error_info& ei) {
    throw as_exception(ei);
}

template<typename T>
using result = result<T, error_info>;

} // namespace pandaproxy::schema_registry
