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
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "container/radix_tree.h"
#include "security/acl.h"
#include "security/acl_entry_set.h"

#include <functional>
#include <memory>
#include <string_view>
#include <tuple>

namespace security {

/*
 * A lightweight, non-owning reference to one (pattern, entry set) pair held by
 * an acl_store. The pair lives in a heap-allocated node owned by `_acls` via a
 * unique_ptr, so these references stay valid across inserts and erasures of
 * *other* patterns (the map may relocate the unique_ptr, but never the node it
 * points at); they are invalidated only when this pattern itself is erased (see
 * acl_store::find()).
 */
struct acl_entry_set_match {
    std::reference_wrapper<const resource_pattern> resource;
    std::reference_wrapper<const acl_entry_set> acl_entry_set;
};

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
            insert_binding(binding).rehash();
        }
    }

    // remove bindings according the input filters and return the bindings that
    // matched in the same order. the `dry_run` flag will identify all of the
    // bindings to be removed but not perform the destructive operation.
    chunked_vector<chunked_vector<acl_binding>> remove_bindings(
      const chunked_vector<acl_binding_filter>&, bool dry_run = false);

    chunked_vector<acl_binding> acls(const acl_binding_filter&) const;

    /**
     * WARNING: The returned acl_matches holds references into `_acls`. The
     * entry sets live in heap-owned nodes, so these survive inserts and
     * removals of *other* patterns, but reset_bindings() (or erasing a
     * referenced pattern) frees the referents. Do not use an acl_matches across
     * a yield point or any acl_store mutation: doing so may result in UNDEFINED
     * BEHAVIOR.
     */
    acl_matches find(resource_type, const ss::sstring&) const;

    // NOTE: the following functions assume that acl_store doesn't change across
    // yield points.
    ss::future<chunked_vector<acl_binding>> all_bindings() const;
    ss::future<> reset_bindings(const chunked_vector<acl_binding>& bindings);

private:
    /// A lightweight, non-owning key for heterogeneous `_acls` lookups
    struct resource_pattern_probe {
        resource_type _resource;
        std::string_view _name;
        pattern_type _pattern;
        resource_type resource() const { return _resource; }
        std::string_view name() const { return _name; }
        pattern_type pattern() const { return _pattern; }
    };

    /*
     * The (pattern, entry set) pair, heap-allocated and owned by `_acls` so its
     * address is stable for as long as the pattern lives. The prefix index and
     * acl_matches reference these fields directly (see acl_entry_set_match), so
     * they must not move when other patterns are added or removed.
     */
    struct acls_node {
        resource_pattern pattern;
        acl_entry_set entries;
    };

    /*
     * Normalize a resource_pattern, a resource_pattern_probe, or a stored
     * acls_node to a common key: the name is viewed as bytes so a probe and the
     * resource_pattern it stands in for produce an identical key. Sharing this
     * between the hash and the equality keeps the two consistent and enables
     * allocation-free heterogeneous lookups.
     */
    static std::tuple<resource_type, std::string_view, pattern_type>
    pattern_key(const auto& p) {
        return {p.resource(), p.name(), p.pattern()};
    }
    static std::tuple<resource_type, std::string_view, pattern_type>
    pattern_key(const std::unique_ptr<acls_node>& n) {
        return pattern_key(n->pattern);
    }

    struct resource_pattern_hash {
        using is_transparent = void;
        // absl::HashOf already mixes well; tell unordered_dense not to re-mix.
        using is_avalanching = void;
        size_t operator()(const auto& p) const {
            return absl::HashOf(pattern_key(p));
        }
    };
    struct resource_pattern_eq {
        using is_transparent = void;
        bool operator()(const auto& a, const auto& b) const {
            return pattern_key(a) == pattern_key(b);
        }
    };

    /*
     * A chunked_hash_set with heap-owned acls_nodes so references to elements
     * survive inserts and erasures of other patterns. This lets the prefix
     * index hold direct references to the (pattern, entry set) pairs here (see
     * acl_entry_set_match) without being invalidated when unrelated ACLs are
     * added or removed.
     */
    using container_type = chunked_hash_set<
      std::unique_ptr<acls_node>,
      resource_pattern_hash,
      resource_pattern_eq>;

    /*
     * INVARIANT: `_prefix_index` holds references into the `acls_node`s that
     * `_acls` owns. The unique_ptr indirection keeps them valid across inserts
     * and across erasing *other* patterns, but erasing a prefixed pattern that
     * the index references frees its node -> use-after-free in find(). So any
     * erasure of an `_acls` key MUST drop the corresponding `_prefix_index`
     * entry in the same step. remove_bindings() (the only element-erasing path)
     * does exactly this when it prunes an emptied pattern; reset_bindings()
     * clears both.
     */
    container_type _acls;

    /*
     * An index over the prefixed patterns in `_acls`, one radix tree per
     * resource type, keyed by pattern name. find() descends the tree once to
     * collect every prefixed pattern that is a prefix of the queried name --
     * rather than scanning a btree range of same-first-character candidates --
     * and the stored match references each entry set directly, so no further
     * `_acls` lookup is needed.
     *
     * The references are stable because each entry set lives in a heap-owned
     * acls_node, so the index is maintained incrementally: add inserts a
     * reference to the new pattern, remove drops it when (and only when) it
     * prunes the emptied pattern from `_acls`, and reset rebuilds alongside
     * `_acls`. There are few resource types, so this map stays tiny.
     */
    absl::flat_hash_map<resource_type, radix_tree<acl_entry_set_match>>
      _prefix_index;

    /// Insert one binding into `_acls`, and when it creates a new prefixed
    /// pattern, record a reference to it in the prefix index. Returns the entry
    /// set it was added to so callers may rehash(); does not rehash itself.
    acl_entry_set& insert_binding(const acl_binding& binding) {
        const auto& pattern = binding.pattern();
        if (auto it = _acls.find(pattern); it != _acls.end()) {
            (*it)->entries.insert(binding.entry());
            return (*it)->entries;
        }
        auto [it, _] = _acls.insert(std::make_unique<acls_node>(pattern));
        auto& node = **it;
        node.entries.insert(binding.entry());
        if (pattern.pattern() == pattern_type::prefixed) {
            _prefix_index[pattern.resource()].insert(
              node.pattern.name(),
              acl_entry_set_match{node.pattern, node.entries});
        }
        return node.entries;
    }
};

/*
 * A lightweight view of references to ACL entries. An instance of this
 * object is created when authorizing and contains ACL matches for the
 * authorization request. Then authorization step searches through the matches
 * based on configured policies and type of authorization request.
 */
class acl_matches {
public:
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
