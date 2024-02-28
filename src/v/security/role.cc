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

#include "security/role.h"

namespace security {
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

// TODO(oren): maybe we should have the role name on the role?
std::ostream& operator<<(std::ostream& os, const role& r) {
    fmt::print(os, "role members: {{{}}}", fmt::join(r.members(), ","));
    return os;
}
} // namespace security
