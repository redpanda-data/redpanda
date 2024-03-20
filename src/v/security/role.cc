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
std::ostream& operator<<(std::ostream& os, role_member_type t) {
    switch (t) {
    case role_member_type::user:
        return os << "User";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const role_member& m) {
    fmt::print(os, "{{{}}}:{{{}}}", m.type(), m.name());
    return os;
}

// TODO(oren): maybe we should have the role name on the role?
std::ostream& operator<<(std::ostream& os, const role& r) {
    fmt::print(os, "role members: {{{}}}", fmt::join(r.members(), ","));
    return os;
}
} // namespace security
