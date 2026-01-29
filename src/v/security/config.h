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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <vector>

namespace security {

std::optional<ss::sstring>
validate_kerberos_mapping_rules(const std::vector<ss::sstring>& r) noexcept;

}

namespace security::tls {

std::optional<ss::sstring>
validate_rules(const std::optional<std::vector<ss::sstring>>& r) noexcept;

}

namespace security::oidc {

/// \brief Defines behavior for nested groups in group claim
enum class nested_group_behavior : uint8_t {
    /// No special handling for nested groups
    none,
    /// Uses suffix of group claim, so `/a/b/c` becomes `c`
    suffix
};

constexpr std::string_view to_string_view(nested_group_behavior b) {
    switch (b) {
    case nested_group_behavior::none:
        return "none";
    case nested_group_behavior::suffix:
        return "suffix";
    }
    return "unknown";
}

static constexpr auto acceptable_nested_group_behavior_values() {
    return std::to_array(
      {to_string_view(nested_group_behavior::none),
       to_string_view(nested_group_behavior::suffix)});
}

std::ostream& operator<<(std::ostream& os, nested_group_behavior b);
std::istream& operator>>(std::istream& is, nested_group_behavior& b);

std::optional<ss::sstring>
validate_principal_mapping_rule(const ss::sstring& rule);

/// \brief Validates the path to the group claim in the OIDC JWT.
///
/// \returns std::nullopt on no error, ss::sstring with error message
std::optional<ss::sstring> validate_group_claim_path(const ss::sstring& path);

} // namespace security::oidc
