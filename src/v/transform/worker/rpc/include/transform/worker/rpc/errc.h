/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <system_error>

namespace transform::worker::rpc {

enum class errc {
    success = 0,
    failed_to_start_engine,
    vm_not_found,
    transform_failed,
    network_error,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final;
    std::string message(int c) const final;
};

inline const std::error_category& error_category() noexcept {
    constinit static errc_category e;
    return e;
}

inline std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}

} // namespace transform::worker::rpc

namespace std {
template<>
struct is_error_code_enum<transform::worker::rpc::errc> : true_type {};
} // namespace std
