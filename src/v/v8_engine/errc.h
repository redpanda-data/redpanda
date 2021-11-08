/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <ostream>
#include <system_error>

namespace v8_engine {

enum class errc { success = 0, v8_runtimes_not_exist, v8_runtimes_compiling };

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "coproc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::v8_runtimes_not_exist:
            return "V8 runtimes does not exist";
        case errc::v8_runtimes_compiling:
            return "V8 runtimes are compiling";
        default:
            return "Undefined coprocessor error encountered";
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}

inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}

} // namespace v8_engine

namespace std {
template<>
struct is_error_code_enum<v8_engine::errc> : true_type {};

} // namespace std
