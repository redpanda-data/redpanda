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
#include "reflection/adl.h"
#include "security/acl.h"
#include "security/types.h"
#include "serde/envelope.h"

#include <iterator>

namespace security {

enum class role_member_type {
    user = 0,
};

// TODO(oren): operator<<

class role_member
  : public serde::
      envelope<role_member, serde::version<0>, serde::compat_version<0>> {
public:
    role_member() = default;
    role_member(role_member_type type, ss::sstring name)
      : _type(type)
      , _name(std::move(name)) {}

    bool operator==(const role_member&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const role_member& e) {
        return H::combine(std::move(h), e._type, e._name);
    }

    // TODO(oren): add operator<<

    const ss::sstring& name() const { return _name; }
    role_member_type type() const { return _type; }

    auto serde_fields() { return std::tie(_type, _name); }

private:
    role_member_type _type{};
    ss::sstring _name;
};

class role
  : public serde::envelope<role, serde::version<0>, serde::compat_version<0>> {
public:
    // TODO(oren): evaluate flat vs node-based set
    using container_type = absl::flat_hash_set<role_member>;

    role() = default;

    // TODO(oren): In my head, I think that we'll be constructing a new role
    // when we update, serializing that into a command then applying it to the
    // controller.
    explicit role(container_type members)
      : _members(std::move(members)) {}

    const container_type& members() const { return _members; }

    bool operator==(const role&) const = default;

    auto serde_fields() { return std::tie(_members); }

private:
    container_type _members;
};

} // namespace security
