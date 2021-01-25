/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "utils/unresolved_address.h"

#include <model/metadata.h>

#include <cstdint>

namespace config {
struct seed_server {
    unresolved_address addr;
};
} // namespace config

namespace std {
static inline ostream& operator<<(ostream& o, const config::seed_server& s) {
    fmt::print(o, "addr: {}", s.addr);
    return o;
}
} // namespace std
