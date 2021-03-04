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
#include "outcome.h"

namespace kafka {

enum class errc {
    success = 0,
    invalid_credentials,
    invalid_scram_state,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "kafka_security::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "kafka_security: Success";
        case errc::invalid_credentials:
            return "kafka_security: Invalid credentials";
        case errc::invalid_scram_state:
            return "kafka_security: Invalid SCRAM state";
        default:
            return "kafka_security: Unknown error";
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
} // namespace kafka

namespace std {
template<>
struct is_error_code_enum<kafka::errc> : true_type {};
} // namespace std
