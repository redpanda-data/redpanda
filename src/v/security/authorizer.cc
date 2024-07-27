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

#include "acl_store.h"
#include "config/configuration.h"
#include "kafka/protocol/types.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "security/role.h"
#include "security/role_store.h"

#include <fmt/core.h>

#include <optional>
#include <ranges>

namespace {

enum class authz_result { empty = 0, deny, allow };

constexpr std::string_view as_string_view(authz_result r) {
    switch (r) {
    case authz_result::empty:
        return "empty";
    case authz_result::deny:
        return "deny";
    case authz_result::allow:
        return "allow";
    }
}

} // namespace

namespace security {

class authorizer::probe {
    using authz_results = std::array<size_t, 3>;

public:
    void setup_metrics() {
        namespace sm = ss::metrics;
        auto add_group = [this](metrics::metric_groups_base& metrics) {
            const auto make_counter = [this](authz_result r) {
                return sm::make_counter(
                         "result",
                         _authz_results[static_cast<size_t>(r)],
                         sm::description(
                           "Total number of authorization results by type"),
                         {sm::label("type")(as_string_view(r))})
                  .aggregate({sm::shard_label});
            };
            metrics.add_group(
              prometheus_sanitize::metrics_name("authorization"),
              {make_counter(authz_result::empty),
               make_counter(authz_result::deny),
               make_counter(authz_result::allow)});
        };

        if (!config::shard_local_cfg().disable_metrics()) {
            add_group(_metrics);
        }
        if (!config::shard_local_cfg().disable_public_metrics()) {
            add_group(_public_metrics);
        }
    }

    constexpr void record_authz_result(authz_result r) {
        ++_authz_results[static_cast<size_t>(r)];
    }

private:
    authz_results _authz_results{};
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

authorizer::~authorizer() = default;

authorizer::authorizer(
  config::binding<std::vector<ss::sstring>> superusers, const role_store* roles)
  : authorizer(allow_empty_matches::no, std::move(superusers), roles) {}

authorizer::authorizer(
  allow_empty_matches allow,
  config::binding<std::vector<ss::sstring>> superusers,
  const role_store* roles)
  : _store(std::make_unique<acl_store>())
  , _superusers_conf(std::move(superusers))
  , _allow_empty_matches(allow)
  , _role_store(roles)
  , _probe(std::make_unique<probe>()) {
    _probe->setup_metrics();
    update_superusers();
    _superusers_conf.watch([this]() { update_superusers(); });
}

void authorizer::add_bindings(const std::vector<acl_binding>& bindings) {
    if (unlikely(
          seclog.is_shard_zero() && seclog.is_enabled(ss::log_level::debug))) {
        for (const auto& binding : bindings) {
            vlog(seclog.debug, "Adding ACL binding: {}", binding);
        }
    }
    store().add_bindings(bindings);
}

std::vector<std::vector<acl_binding>> authorizer::remove_bindings(
  const std::vector<acl_binding_filter>& filters, bool dry_run) {
    return store().remove_bindings(filters, dry_run);
}

std::vector<acl_binding>
authorizer::acls(const acl_binding_filter& filter) const {
    return store().acls(filter);
}

ss::future<fragmented_vector<acl_binding>> authorizer::all_bindings() const {
    return store().all_bindings();
}

ss::future<>
authorizer::reset_bindings(const fragmented_vector<acl_binding>& bindings) {
    return store().reset_bindings(bindings);
}

acl_store& authorizer::store() & { return *_store; }
const acl_store& authorizer::store() const& { return *_store; }

std::ostream& operator<<(std::ostream& os, const auth_result& a) {
    fmt::print(
      os,
      "{{authorized:{}, authorization_disabled:{}, is_superuser:{}, "
      "operation: {}, empty_matches:{}, principal:{}, role:{}, host:{}, "
      "resource_type:{}, "
      "resource_name:{}, resource_pattern:{}, acl:{}}}",
      a.authorized,
      a.authorization_disabled,
      a.is_superuser,
      a.operation,
      a.empty_matches,
      a.principal,
      a.role,
      a.host,
      a.resource_type,
      a.resource_name,
      a.resource_pattern,
      a.acl);

    return os;
}

template<typename T>
auth_result authorizer::authorized(
  const T& resource_name,
  acl_operation operation,
  const acl_principal& principal,
  const acl_host& host) const {
    auth_result r = do_authorized(resource_name, operation, principal, host);
    _probe->record_authz_result(
      r.is_authorized() ? authz_result::allow
      : r.empty_matches ? authz_result::empty
                        : authz_result::deny);
    return r;
}

template<typename T>
auth_result authorizer::do_authorized(
  const T& resource_name,
  acl_operation operation,
  const acl_principal& principal,
  const acl_host& host) const {
    auto type = get_resource_type<T>();
    auto acls = store().find(type, resource_name());

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
          !role
            || (*role != nullptr && (*role)->type() == principal_type::role),
          "Role principal should be non-null and have 'role' type if "
          "present");
        const acl_principal_base& to_check = *role.value_or(&user);
        bool is_allow = perm == acl_permission::allow;
        std::optional<security::acl_match> entry;
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

std::optional<security::acl_match> authorizer::acl_any_implied_ops_allowed(
  const acl_matches& acls,
  const acl_principal_base& principal,
  const acl_host& host,
  const acl_operation operation) const {
    auto check_op = [&acls, &principal, &host](
                      auto begin,
                      auto end) -> std::optional<security::acl_match> {
        for (; begin != end; ++begin) {
            if (auto entry = acls.find(
                  *begin, principal, host, acl_permission::allow);
                entry.has_value()) {
                return entry;
            }
        }

        return {};
    };

    switch (operation) {
    case acl_operation::describe: {
        static constexpr std::array ops = {
          acl_operation::describe,
          acl_operation::read,
          acl_operation::write,
          acl_operation::remove,
          acl_operation::alter,
        };
        return check_op(ops.begin(), ops.end());
    }
    case acl_operation::describe_configs: {
        static constexpr std::array ops = {
          acl_operation::describe_configs,
          acl_operation::alter_configs,
        };
        return check_op(ops.begin(), ops.end());
    }
    default:
        return acls.find(operation, principal, host, acl_permission::allow);
    }
}

} // namespace security
