// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/human.h"

#include "seastarx.h"

#include <seastar/core/print.hh>

#include <ostream>

namespace human {

std::ostream& operator<<(std::ostream& o, const ::human::latency& l) {
    static const char* units[] = {"μs", "ms", "secs"};
    static constexpr double step = 1000;
    auto x = l.value;
    for (size_t i = 0; i < 3; ++i) {
        if (x <= step) {
            return ss::fmt_print(o, "{:03.3f}{}", x, units[i]);
        }
        x /= step;
    }
    return o << x << "uknown_units";
}
std::ostream& operator<<(std::ostream& o, const ::human::bytes& l) {
    static const char* units[] = {"bytes", "MiB", "GiB", "TiB", "PiB"};
    static constexpr double step = 1024;
    auto x = l.value;
    for (size_t i = 0; i < 5; ++i) {
        if (x <= step) {
            return ss::fmt_print(o, "{:03.3f}{}", x, units[i]);
        }
        x /= step;
    }
    return o << x << "uknown_units";
}

} // namespace human
