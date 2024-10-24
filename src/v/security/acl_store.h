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
#include "container/fragmented_vector.h"
#include "security/acl.h"
#include "security/acl_entry_set.h"

#include <absl/container/btree_map.h>

#include <ranges>

namespace security {

/*
 * Container for ACLs.
 */
class acl_matches;
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

    /**
     * WARNING: The acl_matches returned from this function may contain
     * iterators into a container which is NOT iterator stable. Use of these
     * matches across a yield point or acl_store update of any kind may (and
     * likely will) result in UNDEFINED BEHAVIOR.
     */
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

    using container_type = absl::
      btree_map<resource_pattern, acl_entry_set, resource_pattern_compare>;
    container_type _acls;

    /**
     * WARNING: The view returned by this function contains iterators into a
     * container which is NOT iterator stable. Use of this view across a yield
     * point or acl_store update of any kind may (and likely will) result in
     * UNDEFINED BEHAVIOR.
     */
    template<typename RefT>
    static auto get_prefix_view(
      const container_type& acls,
      resource_type resource,
      const ss::sstring& name) {
        auto it = acls.lower_bound(
          resource_pattern(resource, name, pattern_type::prefixed));

        auto end = acls.upper_bound(resource_pattern(
          resource, name.substr(0, 1), pattern_type::prefixed));

        return std::ranges::subrange(it, end)
               | std::views::filter([&name](const auto& e) {
                     return std::string_view(name).starts_with(e.first.name());
                 })
               | std::views::transform(
                 [](const auto& e) { return RefT{e.first, e.second}; });
    }

public:
    template<
      typename RefT,
      typename ViewT = decltype(get_prefix_view<RefT>(
        std::declval<container_type>(),
        std::declval<resource_type>(),
        std::declval<ss::sstring>()))>
    using prefix_view = ViewT;
};

/*
 * A lightweight view of references to ACL entries. An instance of this
 * object is created when authorizing and contains ACL matches for the
 * authorization request. Then authorization step searches through the matches
 * based on configured policies and type of authorization request.
 */
class acl_matches {
public:
    struct acl_entry_set_match {
        std::reference_wrapper<const resource_pattern> resource;
        std::reference_wrapper<const acl_entry_set> acl_entry_set;
    };

    using entry_set_ref = acl_entry_set_match;

    acl_matches(
      std::optional<entry_set_ref> wildcards,
      std::optional<entry_set_ref> literals,
      acl_store::prefix_view<entry_set_ref> prefixes)
      : wildcards(wildcards)
      , literals(literals)
      , prefixes(std::move(prefixes)) {}

    acl_matches(acl_matches&&) noexcept = default;
    acl_matches& operator=(acl_matches&&) noexcept = default;
    acl_matches(const acl_matches&) = delete;
    acl_matches& operator=(const acl_matches&) = delete;
    ~acl_matches() noexcept = default;

    bool empty() const;

    std::optional<acl_match> find(
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

private:
    std::optional<entry_set_ref> wildcards;
    std::optional<entry_set_ref> literals;
    // NOTE(oren): mutable because filter_view & transform_view don't support
    // const iterators as of C++20. Both are slated for C++23, so we can remove
    // the mutable specifier when we bump compilers.
    mutable acl_store::prefix_view<entry_set_ref> prefixes;
};

} // namespace security
