/*
 * Copyright 2020 Vectorized, Inc.
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

namespace utils {

enum class state_crc_file_errc {
    file_not_found,
    crc_mismatch,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "local_state::errc"; }

    std::string message(int c) const final {
        switch (static_cast<state_crc_file_errc>(c)) {
        case state_crc_file_errc::file_not_found:
            return "Requested state file not found";
        case state_crc_file_errc::crc_mismatch:
            return "State file CRC32 mismatch";
        default:
            return "Unknown error";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(state_crc_file_errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}

} // namespace utils

namespace std {
template<>
struct is_error_code_enum<utils::state_crc_file_errc> : true_type {};
} // namespace std