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

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "security/role.h"
#include "security/types.h"

#include <seastar/util/variant_utils.hh>

#include <ranges>
#include <type_traits>

namespace security {

/* container for Roles */
class role_store {
    using role_types = std::variant<role>;
    using container_type = absl::node_hash_map<role_name, role_types>;
    using member_cache_type
      = absl::node_hash_map<role_member, absl::flat_hash_set<role_name>>;

public:
    role_store() noexcept = default;
    role_store(const role_store&) = delete;
    role_store& operator=(const role_store&) = delete;
    role_store(role_store&&) noexcept = default;
    role_store& operator=(role_store&&) noexcept = default;
    ~role_store() noexcept = default;

    template<typename T>
    bool put(const role_name& name, T&& role) {
        auto res = _roles.insert_or_assign(name, std::forward<T>(role));
        using v_type = typename std::remove_cv<
          typename std::remove_reference<T>::type>::type;
        for (const auto& m : std::get<v_type>(res.first->second).members()) {
            _member_cache[m].insert(name);
        }
        return res.second;
    }

    template<typename T>
    std::optional<T> get(const role_name& name) const {
        if (auto it = _roles.find(name); it != _roles.end()) {
            return std::get<T>(it->second);
        }
        return std::nullopt;
    }

    std::optional<const member_cache_type::value_type::second_type*>
    get_member_roles(const role_member& user) const {
        auto it = _member_cache.find(user);
        if (it != _member_cache.end()) {
            return &it->second;
        }
        return std::nullopt;
    }

    bool remove(const role_name& name) {
        absl::c_for_each(
          _member_cache, [&name](auto& e) { e.second.erase(name); });
        return _roles.erase(name) > 0;
    }

    bool contains(const role_name& name) const { return _roles.contains(name); }

    auto range(auto&& pred) const {
        return _roles | std::views::filter(std::forward<decltype(pred)>(pred));
    }

    void clear() {
        _member_cache.clear();
        _roles.clear();
    }

    static constexpr auto name_prefix_filter =
      [](
        const security::role_store::container_type::value_type& t,
        std::string_view filter) {
          return filter.empty() || t.first().starts_with(filter);
      };

    static constexpr auto has_member =
      [](
        const security::role_store::container_type::value_type& t,
        const security::role_member& member) {
          return ss::visit(t.second, [&member](security::role const& r) {
              return member.name().empty() || r.members().contains(member);
          });
      };

private:
    container_type _roles;
    member_cache_type _member_cache;
};

} // namespace security
