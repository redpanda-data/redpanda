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
#include "absl/container/btree_map.h"
#include "container/chunked_vector.h"
#include "security/acl.h"
#include "security/acl_entry_set.h"

#include <string_view>

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

    void add_bindings(const chunked_vector<acl_binding>& bindings) {
        for (auto& binding : bindings) {
            auto& entries = _acls[binding.pattern()];
            entries.insert(binding.entry());
            entries.rehash();
        }
    }

    // remove bindings according the input filters and return the bindings that
    // matched in the same order. the `dry_run` flag will identify all of the
    // bindings to be removed but not perform the destructive operation.
    chunked_vector<chunked_vector<acl_binding>> remove_bindings(
      const chunked_vector<acl_binding_filter>&, bool dry_run = false);

    chunked_vector<acl_binding> acls(const acl_binding_filter&) const;

    /**
     * WARNING: The acl_matches returned from this function may contain
     * iterators into a container which is NOT iterator stable. Use of these
     * matches across a yield point or acl_store update of any kind may (and
     * likely will) result in UNDEFINED BEHAVIOR.
     */
    acl_matches find(resource_type, const ss::sstring&) const;

    // NOTE: the following functions assume that acl_store doesn't change across
    // yield points.
    ss::future<chunked_vector<acl_binding>> all_bindings() const;
    ss::future<> reset_bindings(const chunked_vector<acl_binding>& bindings);

private:
    /// A lightweight, non-owning key for heterogeneous btree lookups
    struct resource_pattern_probe {
        resource_type _resource;
        std::string_view _name;
        pattern_type _pattern;
        resource_type resource() const { return _resource; }
        std::string_view name() const { return _name; }
        pattern_type pattern() const { return _pattern; }
    };

    /*
     * resource pattern ordering:
     *
     *  1. resource type
     *  2. pattern type
     *  3. name (in reverse order)
     */
    struct resource_pattern_compare {
        using is_transparent = void;

        template<typename L, typename R>
        bool operator()(const L& a, const R& b) const {
            if (a.resource() != b.resource()) {
                return a.resource() < b.resource();
            }
            if (a.pattern() != b.pattern()) {
                return a.pattern() < b.pattern();
            }
            return as_view(b.name()) < as_view(a.name());
        }

    private:
        static std::string_view as_view(std::string_view s) { return s; }
        static std::string_view as_view(const ss::sstring& s) {
            return {s.data(), s.size()};
        }
    };

    using container_type = absl::
      btree_map<resource_pattern, acl_entry_set, resource_pattern_compare>;
    container_type _acls;
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

    using prefix_vector = chunked_vector<entry_set_ref>;

    acl_matches(
      std::optional<entry_set_ref> wildcards,
      std::optional<entry_set_ref> literals,
      prefix_vector prefixes)
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
    prefix_vector prefixes;
};

} // namespace security
