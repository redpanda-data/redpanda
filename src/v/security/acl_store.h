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
#include "security/acl.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_set.h>

namespace security {

/*
 * A collection of ACL entries.
 */
class acl_entry_set {
public:
    using container_type = absl::flat_hash_set<acl_entry>;
    using const_iterator = container_type::const_iterator;
    using const_reference = std::reference_wrapper<const acl_entry>;

    acl_entry_set() noexcept = default;
    acl_entry_set(acl_entry_set&&) noexcept = default;
    acl_entry_set& operator=(acl_entry_set&&) noexcept = default;
    acl_entry_set(const acl_entry_set&) = delete;
    acl_entry_set& operator=(const acl_entry_set&) = delete;
    ~acl_entry_set() noexcept = default;

    void insert(acl_entry entry) { _entries.insert(std::move(entry)); }
    void erase(container_type::const_iterator it) { _entries.erase(it); }

    template<typename Predicate>
    void erase_if(Predicate pred) {
        absl::erase_if(_entries, pred);
    }

    void rehash() { _entries.rehash(0); }

    bool empty() const { return _entries.empty(); }

    std::optional<const_reference> find(
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host,
      acl_permission perm) const;

    bool contains(
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host,
      acl_permission perm) const {
        return find(operation, principal, host, perm).has_value();
    }

    const_iterator begin() const { return _entries.cbegin(); }
    const_iterator end() const { return _entries.cend(); }

private:
    container_type _entries;
};

/*
 * A lightweight container of references to ACL entries. An instance of this
 * object is created when authorizing and contains ACL matches for the
 * authorization request. Then authorization step searches through the matches
 * based on configured policies and type of authorization request.
 */
class acl_matches {
public:
    using entry_set_ref = std::reference_wrapper<const acl_entry_set>;

    acl_matches(
      std::optional<entry_set_ref> wildcards,
      std::optional<entry_set_ref> literals,
      std::vector<entry_set_ref> prefixes)
      : wildcards(wildcards)
      , literals(literals)
      , prefixes(std::move(prefixes)) {}

    acl_matches(acl_matches&&) noexcept = default;
    acl_matches& operator=(acl_matches&&) noexcept = default;
    acl_matches(const acl_matches&) = delete;
    acl_matches& operator=(const acl_matches&) = delete;
    ~acl_matches() noexcept = default;

    bool empty() const;

    bool contains(
      acl_operation operation,
      const acl_principal& principal,
      const acl_host& host,
      acl_permission perm) const;

private:
    std::optional<entry_set_ref> wildcards;
    std::optional<entry_set_ref> literals;
    std::vector<entry_set_ref> prefixes;
};

/*
 * Container for ACLs.
 */
class acl_store {
public:
    acl_store() = default;
    acl_store(acl_store&&) noexcept = default;
    acl_store& operator=(acl_store&&) noexcept = default;
    acl_store(const acl_store&) = delete;
    acl_store& operator=(const acl_store&) = delete;
    ~acl_store() noexcept = default;

    void add_bindings(const std::vector<acl_binding>& bindings) {
        for (auto& binding : bindings) {
            auto& entries = _acls[binding.pattern()];
            entries.insert(binding.entry());
            entries.rehash();
        }
    }

    // remove bindings according the input filters and return the bindings that
    // matched in the same order. the `dry_run` flag will identify all of the
    // bindings to be removed but not perform the destructive operation.
    std::vector<std::vector<acl_binding>> remove_bindings(
      const std::vector<acl_binding_filter>&, bool dry_run = false);

    std::vector<acl_binding> acls(const acl_binding_filter&) const;
    acl_matches find(resource_type, const ss::sstring&) const;

    // NOTE: the following functions assume that acl_store doesn't change across
    // yield points.
    ss::future<fragmented_vector<acl_binding>> all_bindings() const;
    ss::future<> reset_bindings(const fragmented_vector<acl_binding>& bindings);

private:
    /*
     * resource pattern ordering:
     *
     *  1. resource type
     *  2. pattern type
     *  3. name (in reverse order)
     */
    struct resource_pattern_compare {
        bool
        operator()(const resource_pattern& a, const resource_pattern& b) const {
            if (a.resource() != b.resource()) {
                return a.resource() < b.resource();
            }
            if (a.pattern() != b.pattern()) {
                return a.pattern() < b.pattern();
            }
            return b.name() < a.name();
        }
    };

    absl::btree_map<resource_pattern, acl_entry_set, resource_pattern_compare>
      _acls;
};

} // namespace security
