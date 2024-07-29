/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "base/vlog.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "security/acl.h"
#include "security/acl_entry_set.h"
#include "security/fwd.h"
#include "security/logger.h"
#include "security/types.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_set.h>

#include <iosfwd>

namespace security {

/**
 * Holds authZ check metadata for audit processing
 */
struct auth_result {
    // Flag indicating if user is authorized
    bool authorized{false};
    // Indicates if the authorization system is disabled
    bool authorization_disabled{false};
    // Indicates if the user is a superuser
    bool is_superuser{false};
    // Indicates if no ACL matches were found
    bool empty_matches{false};

    // If found, the resource pattern that was matched to provide authZ decision
    std::optional<std::reference_wrapper<const resource_pattern>>
      resource_pattern;
    // If found, the ACL that was matched that provided the authZ decision
    std::optional<acl_entry_set::const_reference> acl;
    // The principal that was checked
    security::acl_principal principal;
    // The host
    security::acl_host host;
    // The type of resource
    security::resource_type resource_type;
    // The name of the resource
    ss::sstring resource_name;
    // The operation request
    security::acl_operation operation;

    // If found, the role that was matched to provide authZ decision
    std::optional<security::role_name> role;

    friend std::ostream& operator<<(std::ostream& os, const auth_result& a);

    explicit operator bool() const noexcept { return is_authorized(); }

    bool is_authorized() const noexcept { return authorized; }

    template<typename T>
    static auth_result authz_disabled(
      const security::acl_principal& principal,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource) {
        return {
          .authorized = true,
          .authorization_disabled = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation,
        };
    }

    template<typename T>
    static auth_result superuser_authorized(
      const security::acl_principal& principal,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource) {
        return {
          .authorized = true,
          .is_superuser = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation};
    }

    template<typename T>
    static auth_result empty_match_result(
      const security::acl_principal& principal,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource,
      bool authorized) {
        return {
          .authorized = authorized,
          .empty_matches = true,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation};
    }

    template<typename T>
    static auth_result acl_match(
      const security::acl_principal& principal,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource,
      bool authorized,
      const security::acl_match& match) {
        return {
          .authorized = authorized,
          .resource_pattern = match.resource,
          .acl = match.acl,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation};
    }

    template<typename T>
    static auth_result role_acl_match(
      const security::acl_principal& principal,
      const security::role_name& role,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource,
      bool authorized,
      const security::acl_match& match) {
        return {
          .authorized = authorized,
          .resource_pattern = match.resource,
          .acl = match.acl,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation,
          .role = role};
    }

    template<typename T>
    static auth_result opt_acl_match(
      const security::acl_principal& principal,
      security::acl_host host,
      security::acl_operation operation,
      const T& resource,
      const std::optional<security::acl_match>& match) {
        return {
          .authorized = match.has_value(),
          .empty_matches = !match.has_value(),
          .resource_pattern = match.has_value()
                                ? std::make_optional(match->resource)
                                : std::nullopt,
          .acl = match.has_value() ? std::make_optional(match->acl)
                                   : std::nullopt,
          .principal = principal,
          .host = host,
          .resource_type = get_resource_type<T>(),
          .resource_name = resource(),
          .operation = operation};
    }
};

/*
 * Primary interface for request authorization and management of ACLs.
 *
 * superusers
 * ==========
 *
 * A set of principals may be registered with the authorizer that are allowed to
 * perform any operation. When authorization occurs if the assocaited principal
 * is found in the set of superusers then its request will be permitted. If the
 * principal is not a superuser then normal ACL authorization applies.
 */
class authorizer final {
public:
    // allow operation when no ACL match is found
    using allow_empty_matches = ss::bool_class<struct allow_empty_matches_type>;

    authorizer() = delete;
    ~authorizer();

    authorizer(
      config::binding<std::vector<ss::sstring>> superusers,
      const role_store* roles);

    authorizer(
      allow_empty_matches allow,
      config::binding<std::vector<ss::sstring>> superusers,
      const role_store* roles);

    /*
     * Add ACL bindings to the authorizer.
     */
    void add_bindings(const std::vector<acl_binding>& bindings);

    /*
     * Remove ACL bindings that match the filter(s).
     */
    std::vector<std::vector<acl_binding>> remove_bindings(
      const std::vector<acl_binding_filter>& filters, bool dry_run = false);

    /*
     * Retrieve ACL bindings that match the filter.
     */
    std::vector<acl_binding> acls(const acl_binding_filter& filter) const;

    /*
     * Authorize an operation on a resource. The type of resource is deduced by
     * the type `T` of the name of the resouce (e.g. `model::topic`).
     */
    template<typename T>
    auth_result authorized(
      const T& resource_name,
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host) const;

    ss::future<fragmented_vector<acl_binding>> all_bindings() const;
    ss::future<> reset_bindings(const fragmented_vector<acl_binding>& bindings);

    acl_store& store() &;
    const acl_store& store() const&;

private:
    template<typename T>
    auth_result do_authorized(
      const T& resource_name,
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host) const;

    /*
     * Compute whether the specified operation is allowed based on the implied
     * operations.
     */
    std::optional<security::acl_match> acl_any_implied_ops_allowed(
      const acl_matches& acls,
      const acl_principal_base& principal,
      const acl_host& host,
      const acl_operation operation) const;
    std::unique_ptr<acl_store> _store;

    // The list of superusers is stored twice: once as a vector in the
    // configuration subsystem, then again has a set here for fast lookups.
    // The set is updated on changes via the config::binding.
    absl::flat_hash_set<acl_principal> _superusers;
    config::binding<std::vector<ss::sstring>> _superusers_conf;
    void update_superusers() {
        // Rebuild the whole set, because an incremental change would
        // in any case involve constructing a set to do a comparison
        // between old and new.
        _superusers.clear();
        for (const auto& username : _superusers_conf()) {
            auto principal = acl_principal(principal_type::user, username);
            vlog(seclog.info, "Registered superuser account: {}", principal);
            _superusers.emplace(std::move(principal));
        }
    }

    // NOTE: _allow_empty_matches is used for testing purposes only. In normal
    // operation, an empty match result is ALWAYS unauthorized.
    allow_empty_matches _allow_empty_matches;
    const role_store* _role_store;
    class probe;
    std::unique_ptr<probe> _probe;
};

} // namespace security
