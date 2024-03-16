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

#include "security/authorizer.h"

#include "security/role.h"
#include "security/role_store.h"

#include <optional>
#include <ranges>

namespace security {

template<typename T>
auth_result authorizer::authorized(
  const T& resource_name,
  acl_operation operation,
  const acl_principal& principal,
  const acl_host& host) const {
    auto type = get_resource_type<T>();
    auto acls = _store.find(type, resource_name());

    if (_superusers.contains(principal)) {
        return auth_result::superuser_authorized(
          principal, host, operation, resource_name);
    }

    if (acls.empty()) {
        return auth_result::empty_match_result(
          principal,
          host,
          operation,
          resource_name,
          bool(_allow_empty_matches));
    }

    auto check_access =
      [this, &acls, &operation, &host, &resource_name](
        acl_permission perm,
        const security::acl_principal& user,
        std::optional<const security::acl_principal_base*> role
        = std::nullopt) -> std::optional<auth_result> {
        vassert(
          !role || *role != nullptr && (*role)->type() == principal_type::role,
          "Role principal should be non-null and have 'role' type if "
          "present");
        const acl_principal_base& to_check = *role.value_or(&user);
        bool is_allow = perm == acl_permission::allow;
        std::optional<acl_matches::acl_match> entry;
        if (is_allow) {
            entry = acl_any_implied_ops_allowed(
              acls, to_check, host, operation);
        } else {
            entry = acls.find(operation, to_check, host, perm);
        }
        if (!entry) {
            return std::nullopt;
        }
        switch (to_check.type()) {
        case principal_type::user:
        case principal_type::ephemeral_user:
            return auth_result::acl_match(
              user, host, operation, resource_name, is_allow, *entry);
        case principal_type::role:
            return auth_result::role_acl_match(
              user,
              security::role_name{to_check.name_view()},
              host,
              operation,
              resource_name,
              is_allow,
              *entry);
        }
        __builtin_unreachable();
    };

    auto check_role_access =
      [this, &principal, &check_access](
        acl_permission perm,
        const acl_principal& user) -> std::optional<auth_result> {
        switch (principal.type()) {
        case security::principal_type::user: {
            auto result
              = _role_store->roles_for_member(
                  security::role_member_view::from_principal(principal))
                | std::views::transform(
                  [](const auto& e) { return role::to_principal_view(e); })
                | std::views::transform(
                  [&user, &check_access, perm](const auto& e) {
                      return check_access(perm, user, &e);
                  })
                | std::views::filter([](const std::optional<auth_result>& r) {
                      return r.has_value();
                  })
                | std::views::take(1);
            return (result.empty() ? std::nullopt : result.front());
        }
        case security::principal_type::ephemeral_user:
        case security::principal_type::role:
            return std::nullopt;
        }
        __builtin_unreachable();
    };

    if (auto result = check_access(acl_permission::deny, principal);
        result.has_value()) {
        return std::move(result).value();
    }

    if (auto result = check_role_access(acl_permission::deny, principal);
        result.has_value()) {
        return std::move(result).value();
    }

    if (auto result = check_access(acl_permission::allow, principal);
        result.has_value()) {
        return std::move(result).value();
    }

    if (auto result = check_role_access(acl_permission::allow, principal);
        result.has_value()) {
        return std::move(result).value();
    }

    // NOTE(oren): We know there isn't a match at this point, but I've left
    // this as an opt_acl_match to preserve semantics, namely that this
    // will return a non-authorized result irrespective of the
    // allow_empty_matches flag. On the other hand, switching to an
    // empty_match_result _should_ alter semantics but doesn't break any
    // tests. Not clear whether this is a bug, intended behavior, a gap
    // in test coverage, or a combination.
    return auth_result::opt_acl_match(
      principal, host, operation, resource_name, std::nullopt);
}

template auth_result authorizer::authorized(
  const model::topic&,
  acl_operation,
  const acl_principal&,
  const acl_host&) const;

template auth_result authorizer::authorized(
  const kafka::group_id&,
  acl_operation,
  const acl_principal&,
  const acl_host&) const;

template auth_result authorizer::authorized(
  const security::acl_cluster_name&,
  acl_operation,
  const acl_principal&,
  const acl_host&) const;

template auth_result authorizer::authorized(
  const kafka::transactional_id&,
  acl_operation,
  const acl_principal&,
  const acl_host&) const;

} // namespace security
