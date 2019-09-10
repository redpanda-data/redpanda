#pragma once

#include "seastarx.h"

#include <seastar/core/print.hh>

#include <optional>
#include <ostream>

namespace std {

template<typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        return fmt_print(os, "{{{}}}", *opt);
    }
    return os << "{}";
}

static std::ostream&
operator<<(std::ostream& o, const std::chrono::milliseconds& s) {
    return fmt_print(o, "{}", s.count());
}

} // namespace std
