/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/oidc_principal_mapping_applicator.h"

#include "absl/strings/str_split.h"
#include "container/chunked_vector.h"
#include "security/acl.h"
#include "security/logger.h"
#include "security/oidc_error.h"
#include "security/oidc_principal_mapping.h"

#include <boost/algorithm/string.hpp>
#include <rapidjson/pointer.h>

namespace security::oidc {

namespace detail {
std::expected<chunked_vector<std::string_view>, errc>
get_group_claim(const json::Pointer& p, const jwt& jwt) {
    if (
      auto list_claim = jwt.claim<chunked_vector<std::string_view>>(p);
      list_claim) {
        vlog(
          seclog.trace,
          "Group claim found as string list: {}",
          list_claim.value());
        return std::move(list_claim).value();
    }

    if (auto string_claim = jwt.claim<std::string_view>(p); string_claim) {
        vlog(
          seclog.trace,
          "Group claim found as string: {}",
          string_claim.value());

        chunked_vector<std::string_view> string_claim_parsed;

        auto parts = absl::StrSplit(string_claim.value(), ',');

        for (auto claim : parts) {
            string_claim_parsed.push_back(absl::StripAsciiWhitespace(claim));
        }

        return string_claim_parsed;
    }

    return std::unexpected(errc::group_claim_not_found);
}

acl_principal
apply_nested_group_policy(std::string_view user, nested_group_behavior b) {
    switch (b) {
    case security::oidc::nested_group_behavior::none:
        return acl_principal{principal_type::group, ss::sstring{user}};
    case security::oidc::nested_group_behavior::suffix: {
        auto pos = user.find_last_of('/');
        if (pos == std::string_view::npos) {
            return acl_principal{principal_type::group, ss::sstring{user}};
        }
        return acl_principal{
          principal_type::group, ss::sstring{user.substr(pos + 1)}};
    }
    }
}
} // namespace detail

result<acl_principal> principal_mapping_rule_apply(
  const principal_mapping_rule& mapping, const jwt& jwt) {
    auto claim = jwt.claim<std::string_view>(mapping.claim());
    if (claim.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    auto principal = mapping.mapping().apply(claim.value());
    if (principal.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    return {principal_type::user, std::move(principal).value()};
}

std::expected<chunked_vector<acl_principal>, errc>
group_policy_apply(const group_claim_policy& policy, const jwt& jwt) {
    auto group_claim = detail::get_group_claim(policy.group_pointer(), jwt);
    if (group_claim) {
        return chunked_vector<acl_principal>(
          std::from_range,
          std::views::transform(
            *group_claim,
            [behavior = policy.nested_behavior()](std::string_view g) {
                return detail::apply_nested_group_policy(g, behavior);
            }));
    }

    if (group_claim.error() == errc::group_claim_not_found) {
        if (
          auto claim_names_groups = jwt.claim<std::string_view>(
            json::Pointer("/_claim_names/groups"));
          claim_names_groups) {
            vlog(
              seclog.warn,
              "Azure AD group overage detected: JWT contains "
              "_claim_names.groups instead of actual groups (occurs when "
              "user has >200 groups). Redpanda does not support fetching "
              "groups from external endpoints. Configure Azure AD to limit "
              "groups in the token or use security group filtering");
        }

        vlog(seclog.debug, "Treating missing group claim as empty groups");
        return chunked_vector<acl_principal>{};
    } else {
        return std::unexpected(group_claim.error());
    }
}

} // namespace security::oidc
