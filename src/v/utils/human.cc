// Copyright 2020 Redpanda Data, Inc.
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
    constexpr static std::array units{"μs", "ms", "secs"};
    static constexpr double step = 1000;
    double x = l.value;
    for (const char* unit : units) {
        if (x <= step) {
            fmt::print(o, "{:03.3f}{}", x, unit);
            return o;
        }
        x /= step;
    }
    return o << x << "unknown_units";
}

std::ostream& operator<<(std::ostream& o, const ::human::bytes& l) {
    constexpr static std::array units = {
      "bytes", "KiB", "MiB", "GiB", "TiB", "PiB"};
    static constexpr double step = 1024;
    double x = l.value;
    for (auto& unit : units) {
        if (x <= step) {
            fmt::print(o, "{:03.3f}{}", x, unit);
            return o;
        }
        x /= step;
    }
    return o << x << "unknown_units";
}

} // namespace human
