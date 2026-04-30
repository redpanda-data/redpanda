/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "config/types.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <filesystem>
#include <iostream>
#include <system_error>

namespace cloud_storage_clients {

using access_point_uri = named_type<ss::sstring, struct s3_access_point_uri>;
using object_key = named_type<std::filesystem::path, struct s3_object_key>;
using endpoint_url = named_type<ss::sstring, struct s3_endpoint_url>;
using ca_trust_file
  = named_type<std::filesystem::path, struct s3_ca_trust_file>;

enum class error_outcome {
    retry,
    /// Error condition that couldn't be retried
    fail,
    /// Missing key API error (only suitable for downloads and deletions)
    key_not_found,
    /// Currently used for directory deletion errors in ABS, typically treated
    /// as regular failure outcomes.
    operation_not_supported,
    /// Authentication failed
    authentication_failed,
};

struct error_outcome_category final : public std::error_category {
    const char* name() const noexcept final {
        return "cloud_storage_clients::error_outcome";
    }

    std::string message(int c) const final {
        switch (static_cast<error_outcome>(c)) {
        case error_outcome::retry:
            return "Retryable error";
        case error_outcome::fail:
            return "Non retriable error";
        case error_outcome::key_not_found:
            return "Key not found error";
        case error_outcome::operation_not_supported:
            return "Operation not supported error";
        case error_outcome::authentication_failed:
            return "Authentication failed";
        default:
            return "Undefined error_outcome encountered";
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

enum class s3_url_style { virtual_host = 0, path };

inline fmt::iterator format_to(s3_url_style us, fmt::iterator out) {
    switch (us) {
    case s3_url_style::virtual_host:
        return fmt::format_to(out, "virtual_host");
    case s3_url_style::path:
        return fmt::format_to(out, "path");
    }
}

inline std::optional<s3_url_style>
from_config(std::optional<config::s3_url_style> us) {
    if (us.has_value()) {
        switch (us.value()) {
        case config::s3_url_style::virtual_host:
            return s3_url_style::virtual_host;
        case config::s3_url_style::path:
            return s3_url_style::path;
        }
    }
    return std::nullopt;
}

enum class response_content_type : int8_t { unknown, xml, json };

/// Class of service for client_pool lease acquisition.
///   priority - bypasses the capped-budget gate; always choose this for
///              latency-sensitive callers (e.g. cloud topic write path).
///   capped   - subject to a per-shard capped-budget; never borrows
///              cross-shard. Ideal for cold reads, cache hydration, etc.
enum class lease_class : uint8_t {
    priority,
    capped,
};

} // namespace cloud_storage_clients

namespace std {
template<>
struct is_error_code_enum<cloud_storage_clients::error_outcome> : true_type {};
} // namespace std

template<>
struct fmt::formatter<cloud_storage_clients::error_outcome> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    auto format(
      cloud_storage_clients::error_outcome e, fmt::format_context& ctx) const {
        return fmt::format_to(
          ctx.out(),
          "{}",
          cloud_storage_clients::error_outcome_category{}.message(
            static_cast<int>(e)));
    }
};
