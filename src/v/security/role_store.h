/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "container/fragmented_vector.h"
#include "security/fwd.h"
#include "security/role.h"
#include "security/types.h"

#include <seastar/util/noncopyable_function.hh>

#include <absl/algorithm/container.h>
#include <boost/range/iterator_range.hpp>

#include <ranges>
#include <string_view>
#include <type_traits>

namespace security {

namespace detail {

// Hash and Equal structs for transparent lookups in node_hash_map
struct role_member_hash {
    using is_transparent = std::true_type;

    size_t operator()(role_member_view v) const {
        return absl::Hash<role_member_view>{}(v);
    }
};

struct role_member_eq {
    using is_transparent = std::true_type;

    bool operator()(role_member_view lhs, role_member_view rhs) const {
        return lhs == rhs;
    }
};

} // namespace detail

/*
 * Store for roles and role members.
 *
 * This store is subject to two distinct usage cases:
 *   1. Retrieve the list of roles assigned to some user
 *      - Used inline as part of the request authorization flow
 *   2. Read and write from  the set of all roles
 *      - Used for servicing role management requests in the Admin API
 *
 * The internal structure of role_store is geared toward servicing
 * (1) as efficiently as possible with the primary goal of avoiding
 * performance regression in the authorizer.
 *
 */
class role_store {
    struct role_name_view
      : public named_type<std::string_view, struct role_name_view_tag> {
        explicit role_name_view(const role_name& r)
          : named_type<std::string_view, struct role_name_view_tag>(r()) {}
    };
    using role_set_type = absl::node_hash_set<role_name>;
    using name_view_set_type = absl::flat_hash_set<role_name_view>;
    using members_store_type = absl::node_hash_map<
      role_member,
      name_view_set_type,
      detail::role_member_hash,
      detail::role_member_eq>;
    using role_accessor = std::pair<
      role_name_view, /* role_name */
      ss::noncopyable_function<const members_store_type&(void)>>;
    using range_query_container_type = fragmented_vector<role_name_view>;

public:
    using roles_range
      = boost::iterator_range<name_view_set_type::const_iterator>;

    role_store() noexcept = default;
    role_store(const role_store&) = delete;
    role_store& operator=(const role_store&) = delete;
    role_store(role_store&&) noexcept = default;
    role_store& operator=(role_store&&) noexcept = default;
    ~role_store() noexcept = default;

    // Add the given role and members range iff it is not already present.
    template<typename T>
    requires std::ranges::range<T>
             && std::convertible_to<std::ranges::range_value_t<T>, role_member>
    bool put(role_name name, const T& role) {
        auto [it, inserted] = _roles.insert(std::move(name));
        if (inserted) {
            for (const auto& m : role) {
                _members_store[m].emplace(*it);
            }
        }
        return inserted;
    }

    std::optional<role> get(const role_name& name) const;

    template<RoleMember T>
    roles_range roles_for_member(const T& user) const {
        if (auto it = _members_store.find(user); it != _members_store.end()) {
            return it->second;
        }
        return {};
    }

    bool remove(const role_name& name);
    bool contains(const role_name& name) const { return _roles.contains(name); }
    void clear() {
        _members_store.clear();
        _roles.clear();
    }
    size_t size() const { return _roles.size(); }

    // Retrieve a list of role_names that satisfy some predicate
    //
    // e.g.:
    // store.range([](const auto& r) {
    //     role_member mem{role_member_type::user, "user"};
    //     return role_store::has_member(r, mem) &&
    //       role_store::name_prefix_filter(e, "foo");
    // });
    range_query_container_type
    range(std::function<bool(const role_accessor&)>&& pred) const;

    static constexpr auto name_prefix_filter =
      [](const role_accessor& e, std::string_view filter) {
          const auto& name = e.first;
          return filter.empty() || name().starts_with(filter);
      };

    static constexpr auto has_member =
      [](const role_accessor& e, const RoleMember auto& member) {
          const auto& [name, get_ms] = e;
          const auto& ms = get_ms();
          if (auto it = ms.find(member); it != ms.end()) {
              return it->second.contains(name);
          }
          return false;
      };

private:
    members_store_type _members_store;
    role_set_type _roles;
};

} // namespace security
