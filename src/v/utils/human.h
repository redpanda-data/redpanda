#pragma once

#include <iostream>

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
