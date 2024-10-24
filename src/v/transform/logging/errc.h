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

#include <system_error>

namespace transform::logging {

enum class errc {
    success = 0,
    // When logs topic creation fails
    topic_creation_failure,
    // When logs topic metadata lookup fails
    topic_not_found,
    // When we can't compute a partition ID for some transform's logs
    partition_lookup_failure,
    // When producing logs batches fails
    write_failure,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final {
        return "transform::logging::errc";
    }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "transform::logging::success";
        case errc::topic_creation_failure:
            return "Failed to create transform logs topic";
        case errc::topic_not_found:
            return "Transform logs topic metadata lookup failed";
        case errc::partition_lookup_failure:
            return "Failed to compute output partition ID for transform logs";
        case errc::write_failure:
            return "Failed write to transform logs topic";
        default:
            return "transform::logging::errc::unknown(" + std::to_string(c)
                   + ")";
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

} // namespace transform::logging

namespace std {
template<>
struct is_error_code_enum<transform::logging::errc> : true_type {};
} // namespace std
