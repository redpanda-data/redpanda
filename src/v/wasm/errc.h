/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <system_error>

namespace wasm {
enum class errc {
    success = 0,
    // When the user's code fails to be loaded
    load_failure,
    // When the engine is fails to be created
    engine_creation_failure,
    // When the user's supplied code errors
    user_code_failure,
    // Engine is not running
    engine_not_running,
    // Engine wasm shutdown
    engine_shutdown,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "wasm::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "wasm::errc::success";
        case errc::load_failure:
            return "wasm::errc::load_failure";
        case errc::engine_creation_failure:
            return "wasm::errc::engine_creation_failure";
        case errc::user_code_failure:
            return "wasm::errc::user_code_failure";
        case errc::engine_not_running:
            return "wasm::errc::engine_not_running";
        case errc::engine_shutdown:
            return "wasm::errc::engine_shutdown";
        default:
            return "wasm::errc::unknown(" + std::to_string(c) + ")";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}

class wasm_exception final : public std::exception {
public:
    explicit wasm_exception(std::string msg, errc err_code) noexcept
      : _msg(std::move(msg))
      , _err_code(err_code) {}

    const char* what() const noexcept final { return _msg.c_str(); }

    errc error_code() const noexcept { return _err_code; }

private:
    std::string _msg;
    errc _err_code;
};

} // namespace wasm

namespace std {
template<>
struct is_error_code_enum<wasm::errc> : true_type {};
} // namespace std
