/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <chrono>
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
    // the input should be in microseconds
    explicit latency(double x)
      : value(x) {}
    template<typename Duration>
    explicit latency(Duration d)
      : value(
          std::chrono::duration_cast<std::chrono::microseconds>(d).count()) {}
    double value;
    friend std::ostream& operator<<(std::ostream& o, const latency&);
};
} // namespace human
