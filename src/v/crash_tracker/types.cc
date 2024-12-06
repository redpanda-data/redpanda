/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/types.h"

#include <fmt/core.h>

#include <cstring>
#include <ostream>

namespace crash_tracker {

void crash_description::describe(
  std::ostream& os, bool incl_additional_info) const {
    fmt::print(os, "{}", _crash_message.c_str());

    const auto opt_stacktrace = _stacktrace.c_str();
    const auto has_stacktrace = strlen(opt_stacktrace) > 0;
    if (has_stacktrace) {
        fmt::print(os, " Backtrace: {}.", opt_stacktrace);
    }

    const auto opt_add_info = _addition_info.c_str();
    const auto has_add_info = strlen(opt_add_info) > 0;
    if (incl_additional_info && has_add_info) {
        fmt::print(os, " {}", opt_add_info);
    }
}

void crash_description::describe_short(std::ostream& os) const {
    return describe(os, false);
}

void crash_description::describe_long(std::ostream& os) const {
    return describe(os, true);
}

bool is_crash_loop_limit_reached(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const crash_loop_limit_reached&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace crash_tracker
