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

#include "base/outcome.h"

#include <system_error>

namespace debug_bundle {
/**
 * @brief Error codes for the debug bundle service
 */
enum class error_code : int {
    success = 0,
    debug_bundle_process_running,
    debug_bundle_process_not_running,
    invalid_parameters,
    process_failed,
    internal_error,
    job_id_not_recognized,
    debug_bundle_process_never_started,
    rpk_binary_not_present,
    debug_bundle_expired,
};

std::error_code make_error_code(error_code e) noexcept;

/**
 * @brief Contains information about any errors encountered
 */
class error_info {
public:
    /**
     * @brief Construct a new error_info object with just an error code
     *
     * @param ec Error code
     */
    explicit error_info(error_code ec)
      : _ec(ec)
      , _msg(make_error_code(ec).message()) {}

    /**
     * @brief Construct a new error info object using error code and message
     *
     * @param ec Error code
     * @param msg message
     */
    error_info(error_code ec, std::string msg)
      : _ec(ec)
      , _msg(std::move(msg)) {}

    const error_code& code() const noexcept { return _ec; };
    const std::string& message() const noexcept { return _msg; }

private:
    error_code _ec;
    std::string _msg;
};

/// Result type for debug bundle service
template<typename T>
using result = result<T, error_info>;
} // namespace debug_bundle

namespace std {
template<>
struct is_error_code_enum<debug_bundle::error_code> : true_type {};
} // namespace std
