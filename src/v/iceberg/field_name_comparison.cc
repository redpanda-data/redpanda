/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/field_name_comparison.h"

#include "base/seastarx.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <istream>

namespace iceberg {

fmt::iterator format_to(field_name_comparison n, fmt::iterator out) {
    switch (n) {
    case field_name_comparison::verbatim:
        return fmt::format_to(out, "verbatim");
    case field_name_comparison::lower_case:
        return fmt::format_to(out, "lower_case");
    }
    return fmt::format_to(out, "unknown");
}

std::istream& operator>>(std::istream& is, field_name_comparison& n) {
    ss::sstring s;
    is >> s;
    try {
        n = string_switch<field_name_comparison>(s)
              .match("verbatim", field_name_comparison::verbatim)
              .match("lower_case", field_name_comparison::lower_case);
    } catch (const std::runtime_error&) {
        is.setstate(std::ios::failbit);
    }
    return is;
}

} // namespace iceberg
