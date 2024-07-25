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

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <filesystem>
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
    operation_not_supported
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

} // namespace cloud_storage_clients

namespace std {
template<>
struct is_error_code_enum<cloud_storage_clients::error_outcome> : true_type {};
} // namespace std
