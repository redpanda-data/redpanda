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

#include "security/acl.h"
#include "utils/absl_sstring_hash.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace security {

/*
 * A collection of ACL entries.
 */
class acl_entry_set {
public:
    using container_type = absl::node_hash_set<acl_entry>;
    using role_cache_type
      = absl::flat_hash_map<ss::sstring, ssize_t, sstring_hash, sstring_eq>;
    using const_iterator = container_type::const_iterator;
    using const_reference = std::reference_wrapper<const acl_entry>;

    acl_entry_set() noexcept = default;
    acl_entry_set(acl_entry_set&&) noexcept = default;
    acl_entry_set& operator=(acl_entry_set&&) noexcept = default;
    acl_entry_set(const acl_entry_set&) = delete;
    acl_entry_set& operator=(const acl_entry_set&) = delete;
    ~acl_entry_set() noexcept = default;

    void insert(acl_entry entry);
    void erase(container_type::const_iterator it) {
        remove_if_role(it->principal());
        _entries.erase(it);
    }
    void remove_if_role(const acl_principal_base& p);

    template<typename Predicate>
    void erase_if(Predicate pred) {
        absl::erase_if(_entries, pred);
    }

    void rehash() {
        _entries.rehash(0);
        _role_cache.rehash(0);
    }

    bool empty() const { return _entries.empty(); }

    std::optional<const_reference> find(
      acl_operation operation,
      const acl_principal_base& principal,
      const acl_host& host,
      acl_permission perm) const;

    bool contains(
      acl_operation operation,
      const acl_principal_base& principal,
      const acl_host& host,
      acl_permission perm) const {
        return find(operation, principal, host, perm).has_value();
    }

    const_iterator begin() const { return _entries.cbegin(); }
    const_iterator end() const { return _entries.cend(); }

private:
    container_type _entries;
    role_cache_type _role_cache;
};

struct acl_match {
    std::reference_wrapper<const resource_pattern> resource;
    acl_entry_set::const_reference acl;
};
} // namespace security
