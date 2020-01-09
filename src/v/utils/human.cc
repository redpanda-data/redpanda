#include "utils/human.h"

#include "seastarx.h"

#include <seastar/core/print.hh>

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
