/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "security/ephemeral_credential.h"

#include <fmt/ostream.h>

namespace security {

std::ostream& operator<<(std::ostream& os, const ephemeral_credential& c) {
    fmt::print(os, "principal: {}, user: {}", c.principal(), c.user());
    return os;
}

} // namespace security
