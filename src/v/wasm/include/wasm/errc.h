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
    // invalid module without wasi _start
    invalid_module_missing_wasi,
    // invalid module without known abi marker
    invalid_module_missing_abi,
    // invalid module with unsupported schema registry support
    invalid_module_unsupported_sr,
    // invalid module otherwise
    invalid_module,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "wasm::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::load_failure:
            return "WebAssembly load failure";
        case errc::engine_creation_failure:
            return "WebAssembly engine creation failure";
        case errc::user_code_failure:
            return "WebAssembly user code failure";
        case errc::engine_not_running:
            return "WebAssembly not running";
        case errc::engine_shutdown:
            return "WebAssembly engine shutdown";
        case errc::invalid_module_missing_wasi:
            return "invalid WebAssembly - not compiled for wasi/wasm32";
        case errc::invalid_module_missing_abi:
            return "invalid WebAssembly - invalid SDK";
        case errc::invalid_module_unsupported_sr:
            return "invalid WebAssembly - invalid schema registry SDK";
        case errc::invalid_module:
            return "invalid WebAssembly";
        }
        return "wasm::errc::unknown(" + std::to_string(c) + ")";
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
