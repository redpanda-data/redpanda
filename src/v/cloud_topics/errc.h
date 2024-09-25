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

namespace experimental::cloud_topics {

enum class errc : int16_t {
    success,
    timeout,
    upload_failure,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cloud_topics:errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "OK";
        case errc::timeout:
            return "timeout";
        case errc::upload_failure:
            return "upload_failure";
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
} // namespace experimental::cloud_topics
namespace std {
template<>
struct is_error_code_enum<cloud_topics::errc> : true_type {};
} // namespace std
