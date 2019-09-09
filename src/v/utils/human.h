#pragma once

#include <fmt/ostream.h>
#include <fmt/format.h>

/// \brief usage: fmt::format("{}", human::bytes(3234.234));
namespace human {
struct bytes {
    bytes(double x)
      : value(x) {
    }
    const double value;
};
struct latency {
    latency(double x)
      : value(x) {
    }
    const double value;
};

} // namespace human
inline std::ostream& operator<<(std::ostream& o, ::human::latency l) {
  static const char* units[] = {"μs", "ms", "secs"};
  static constexpr double step = 1000;
  double x = l.value;
  for (size_t i = 0; i < 3; ++i) {
    if (x <= step) {
      return o << fmt::format("{:03.3f}{}", x, units[i]);
    }
    x /= step;
  }
  return o << x << "uknown_units";
}
inline std::ostream& operator<<(std::ostream& o, ::human::bytes l) {
  static const char* units[] = {"bytes", "MiB", "GiB", "TiB", "PiB"};
  static constexpr double step = 1024;
  double x = l.value;
  for (size_t i = 0; i < 5; ++i) {
    if (x <= step) {
      return o << fmt::format("{:03.3f}{}", x, units[i]);
    }
    x /= step;
  }
  return o << x << "uknown_units";
}
