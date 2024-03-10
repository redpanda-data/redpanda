/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/role.h"

namespace security {

namespace {
role_member_type member_type_for_principal_type(security::principal_type p) {
    switch (p) {
    case security::principal_type::user:
        return role_member_type::user;
    case security::principal_type::ephemeral_user:
    case security::principal_type::role:
        vassert(false, "Invalid principal_type {{{}}} for role membership", p);
    }
    __builtin_unreachable();
}
} // namespace

std::ostream& operator<<(std::ostream& os, role_member_type t) {
    switch (t) {
    case role_member_type::user:
        return os << "user";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const role_member& m) {
    fmt::print(os, "{{{}}}:{{{}}}", m.type(), m.name());
    return os;
}

role_member role_member::from_principal(const security::acl_principal& p) {
    return {member_type_for_principal_type(p.type()), ss::sstring{p.name()}};
}

// TODO(oren): maybe we should have the role name on the role?
std::ostream& operator<<(std::ostream& os, const role& r) {
    fmt::print(os, "role members: {{{}}}", fmt::join(r.members(), ","));
    return os;
}

security::acl_principal role::to_principal(std::string_view role_name) {
    return {
      security::principal_type::role, {role_name.data(), role_name.size()}};
}

security::acl_principal_view
role::to_principal_view(std::string_view role_name) {
    return {security::principal_type::role, role_name};
}

} // namespace security
