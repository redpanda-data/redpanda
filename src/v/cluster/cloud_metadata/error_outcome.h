/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include <fmt/core.h>

#include <system_error>

namespace cluster::cloud_metadata {

enum class error_outcome {
    success = 0,
    list_failed,
    download_failed,
    upload_failed,
    no_matching_metadata,
    term_has_changed,
    not_ready,
    ntp_not_found,
    rpc_error,
};

struct error_outcome_category final : public std::error_category {
    const char* name() const noexcept final {
        return "cluster::cloud_metadata::errc";
    }

    std::string message(int c) const final {
        switch (static_cast<error_outcome>(c)) {
        case error_outcome::success:
            return "Success";
        case error_outcome::list_failed:
            return "List objects failed";
        case error_outcome::download_failed:
            return "Download object failed";
        case error_outcome::upload_failed:
            return "Upload object failed";
        case error_outcome::no_matching_metadata:
            return "No matching metadata";
        case error_outcome::term_has_changed:
            return "Term has changed";
        case error_outcome::not_ready:
            return "Not ready";
        case error_outcome::ntp_not_found:
            return "NTP not found";
        case error_outcome::rpc_error:
            return "RPC error";
        default:
            return fmt::format("Unknown outcome ({})", c);
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static error_outcome_category e;
    return e;
}

inline std::error_code make_error_code(error_outcome e) noexcept {
    return {static_cast<int>(e), error_category()};
}

inline std::ostream& operator<<(std::ostream& o, error_outcome e) {
    o << error_category().message(static_cast<int>(e));
    return o;
}

} // namespace cluster::cloud_metadata

namespace std {
template<>
struct is_error_code_enum<cluster::cloud_metadata::error_outcome>
  : true_type {};
} // namespace std
