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

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>

#include <optional>
#include <ostream>

namespace std {

template<typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        fmt::print(os, "{{{}}}", *opt);
        return os;
    }
    return os << "{nullopt}";
}

static inline std::ostream&
operator<<(std::ostream& o, const ss::lowres_clock::duration& d) {
    fmt::print(o, "{}", d.count());
    return o;
}

} // namespace std
