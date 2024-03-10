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
#include "security/acl.h"
#include "security/types.h"
#include "serde/envelope.h"

#include <seastar/core/sstring.hh>

#include <iosfwd>

namespace security {

enum class role_member_type {
    user = 0,
};

std::ostream& operator<<(std::ostream&, role_member_type);

class role_member
  : public serde::
      envelope<role_member, serde::version<0>, serde::compat_version<0>> {
public:
    // NOTE(oren): Default constructor is for serde tag_invoke only.
    // A default constructed `role_member` is not meaningful and you shouldn't
    // construct them.
    role_member() = default;
    role_member(role_member_type type, ss::sstring name)
      : _type(type)
      , _name(std::move(name)) {}

    std::string_view name() const { return _name; }
    role_member_type type() const { return _type; }

    auto serde_fields() { return std::tie(_type, _name); }

    static role_member from_principal(const security::acl_principal& p);

private:
    friend bool operator==(const role_member&, const role_member&) = default;
    friend std::ostream& operator<<(std::ostream&, const role_member&);

    template<typename H>
    friend H AbslHashValue(H h, const role_member& e) {
        return H::combine(std::move(h), e._type, e._name);
    }

    role_member_type _type{};
    ss::sstring _name;
};

class role
  : public serde::envelope<role, serde::version<0>, serde::compat_version<0>> {
public:
    using container_type = absl::flat_hash_set<role_member>;

    role() = default;

    explicit role(container_type members)
      : _members(std::move(members)) {}

    const container_type& members() const& { return _members; }
    container_type&& members() && { return std::move(_members); }

    auto serde_fields() { return std::tie(_members); }

    auto begin() const { return _members.begin(); }
    auto end() const { return _members.end(); }
    auto cbegin() const { return _members.cbegin(); }
    auto cend() const { return _members.cend(); }

    /**
     * Construct a concrete acl_principal of principal_type::role
     */
    static security::acl_principal to_principal(std::string_view role_name);
    /**
     * Construct a concrete acl_principal_view of principal_type::role
     */
    static security::acl_principal_view
    to_principal_view(std::string_view role_name);

private:
    friend bool operator==(const role&, const role&) = default;
    friend std::ostream& operator<<(std::ostream&, const role&);

    container_type _members;
};

} // namespace security
