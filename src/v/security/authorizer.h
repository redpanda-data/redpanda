/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "security/acl.h"
#include "security/acl_store.h"
#include "security/logger.h"
#include "vlog.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/core.h>

namespace security {

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

    authorizer()
      : authorizer(allow_empty_matches::no) {}

    explicit authorizer(allow_empty_matches allow)
      : _allow_empty_matches(allow) {}

    /*
     * Add ACL bindings to the authorizer.
     */
    void add_bindings(const std::vector<acl_binding>& bindings) {
        if (unlikely(
              seclog.is_shard_zero()
              && seclog.is_enabled(ss::log_level::debug))) {
            for (const auto& binding : bindings) {
                vlog(seclog.debug, "Adding ACL binding: {}", binding);
            }
        }
        _store.add_bindings(bindings);
    }

    /*
     * Remove ACL bindings that match the filter(s).
     */
    std::vector<std::vector<acl_binding>> remove_bindings(
      const std::vector<acl_binding_filter>& filters, bool dry_run = false) {
        return _store.remove_bindings(filters, dry_run);
    }

    /*
     * Retrieve ACL bindings that match the filter.
     */
    std::vector<acl_binding> acls(const acl_binding_filter& filter) const {
        return _store.acls(filter);
    }

    /*
     * Authorize an operation on a resource. The type of resource is deduced by
     * the type `T` of the name of the resouce (e.g. `model::topic`).
     */
    template<typename T>
    bool authorized(
      const T& resource_name,
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host) const {
        auto type = get_resource_type<T>();
        auto acls = _store.find(type, resource_name());

        if (_superusers.contains(principal)) {
            return true;
        }

        if (acls.empty()) {
            return bool(_allow_empty_matches);
        }

        // check for deny
        if (acls.contains(operation, principal, host, acl_permission::deny)) {
            return false;
        }

        // check for allow
        auto ops = acl_implied_ops(operation);
        return std::any_of(
          ops.cbegin(),
          ops.cend(),
          [&acls, &principal, &host](acl_operation operation) {
              return acls.contains(
                operation, principal, host, acl_permission::allow);
          });
    }

    void add_superuser(acl_principal principal) {
        if (unlikely(seclog.is_shard_zero())) {
            vlog(seclog.debug, "Adding superuser: {}", principal);
        }
        _superusers.emplace(std::move(principal));
    }

private:
    acl_store _store;
    absl::flat_hash_set<acl_principal> _superusers;
    allow_empty_matches _allow_empty_matches;
};

} // namespace security
