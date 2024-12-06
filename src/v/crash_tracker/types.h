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

#pragma once

#include "base/seastarx.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"

#include <exception>
#include <ostream>

namespace crash_tracker {

enum class crash_type {
    unknown,
    startup_exception,
    segfault,
    abort,
    illegal_instruction
};

struct crash_description
  : serde::
      envelope<crash_description, serde::version<0>, serde::compat_version<0>> {
    crash_type _type;
    model::timestamp _crash_time;
    ss::sstring _crash_message;
    ss::sstring _stacktrace;

    /// Extension to the _crash_message. It can be used to add further
    /// information about the crash that is useful for debugging but is too
    /// verbose for telemetry.
    /// Eg. top-N allocations
    ss::sstring _addition_info;

    void describe_short(std::ostream&) const;
    void describe_long(std::ostream&) const;

    auto serde_fields() {
        return std::tie(
          _type, _crash_time, _crash_message, _stacktrace, _addition_info);
    }

private:
    void describe(std::ostream&, bool) const;
};

struct crash_tracker_metadata
  : serde::envelope<
      crash_tracker_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    uint32_t _crash_count{0};
    uint64_t _config_checksum{0};
    model::timestamp _last_start_ts;

    auto serde_fields() {
        return std::tie(_crash_count, _config_checksum, _last_start_ts);
    }
};

class crash_loop_limit_reached : public std::runtime_error {
public:
    explicit crash_loop_limit_reached()
      : std::runtime_error("Crash loop detected, aborting startup.") {}
};

bool is_crash_loop_limit_reached(std::exception_ptr);

} // namespace crash_tracker
