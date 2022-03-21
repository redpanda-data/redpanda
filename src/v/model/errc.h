/*
 * Copyright 2020 Redpanda Data, Inc.
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

namespace model {

enum class errc {
    success = 0,
    topic_name_len_exceeded,
    topic_name_empty,
    invalid_topic_name,
    forbidden_topic_name
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "model::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::topic_name_len_exceeded:
            return "Invalid topic name: max length 249 exceeded for";
        case errc::topic_name_empty:
            return "Invalid topic name: cannot be empty.";
        case errc::invalid_topic_name:
            return "Invalid topic name validation pattern [a-zA-Z0-9_\\.\\-] "
                   "failed for";
        case errc::forbidden_topic_name:
            return "Invalid topic name:";
        default:
            return "model::errc::unknown";
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
} // namespace model
namespace std {
template<>
struct is_error_code_enum<model::errc> : true_type {};
} // namespace std
