#pragma once

#include <iostream>
/// \brief usage: fmt::format("{}", human::bytes(3234.234));
namespace human {
struct bytes {
    bytes(double x)
      : value(x) {
    }
    double value;
};
struct latency {
    latency(double x)
      : value(x) {
    }
    double value;
};
} // namespace human
std::ostream& operator<<(std::ostream& o, const ::human::latency&);
std::ostream& operator<<(std::ostream& o, const ::human::bytes&);
