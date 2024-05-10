// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include <system_error>

namespace storage::experimental::mvlog {

enum class errc {
    none = 0,

    // Indicates that there is something unexpected about the logical data.
    broken_data_invariant,

    // Checksum error, likely corruption.
    checksum_mismatch,

    // Not enough data in a stream, potentially corruption.
    short_read,

};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final {
        return "storage::experimental::mvlog";
    }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::none:
            return "none";
        case errc::broken_data_invariant:
            return "broken_data_invariant";
        case errc::checksum_mismatch:
            return "checksum_mismatch";
        case errc::short_read:
            return "short_read";
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

inline std::ostream& operator<<(std::ostream& o, errc e) {
    o << error_category().message(static_cast<int>(e));
    return o;
}

} // namespace storage::experimental::mvlog

namespace std {
template<>
struct is_error_code_enum<storage::experimental::mvlog::errc> : true_type {};
} // namespace std
