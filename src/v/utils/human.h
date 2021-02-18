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

#include <iosfwd>

/// \brief usage: fmt::format("{}", human::bytes(3234.234));
//  or fmt::format("{}", human::latency(321.048));
namespace human {
struct bytes {
    explicit bytes(double x)
      : value(x) {}
    double value;
    friend std::ostream& operator<<(std::ostream& o, const bytes&);
};
struct latency {
    explicit latency(double x)
      : value(x) {}
    double value;
    friend std::ostream& operator<<(std::ostream& o, const latency&);
};
} // namespace human
