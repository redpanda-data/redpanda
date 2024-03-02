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

#include "absl/container/node_hash_map.h"
#include "security/role.h"
#include "security/types.h"

#include <seastar/util/variant_utils.hh>

#include <boost/range/adaptor/filtered.hpp>

namespace security {

/* container for Roles */
class role_store {
    // TODO(oren): is it worth having a variant here? could "groups" sort of
    // fall into this category?
    using role_types = std::variant<role>;
    using container_type = absl::node_hash_map<role_name, role_types>;

public:
    role_store() noexcept = default;
    role_store(const role_store&) = delete;
    role_store& operator=(const role_store&) = delete;
    role_store(role_store&&) noexcept = default;
    role_store& operator=(role_store&&) noexcept = default;
    ~role_store() noexcept = default;

    template<typename T>
    void put(const role_name& name, T&& role) {
        _roles.insert_or_assign(name, std::forward<T>(role));
    }

    template<typename T>
    auto get(const role_name& name) -> std::optional<T> const {
        if (auto it = _roles.find(name); it != _roles.end()) {
            return std::get<T>(it->second);
        }
        return std::nullopt;
    }

    bool remove(const role_name& name) { return _roles.erase(name) > 0; }

    bool contains(const role_name& name) const { return _roles.contains(name); }

    // TODO(oren): concept of ephemeral creds has special treatment in
    // credentials_store. Don't think we need something like that here.

    auto range(auto pred) {
        return boost::adaptors::filter(_roles, std::move(pred));
    }

    void clear() { _roles.clear(); }

    static constexpr auto name_prefix_filter =
      [](
        const security::role_store::container_type::value_type& t,
        const ss::sstring& filter) {
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
};

} // namespace security
