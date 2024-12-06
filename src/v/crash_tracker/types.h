/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "bytes/iobuf.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace crash_tracker {

struct crash_description
  : serde::
      envelope<crash_description, serde::version<0>, serde::compat_version<0>> {
    model::timestamp _crash_time;
    ss::sstring _crash_reason;
    std::vector<uint64_t> _stacktrace;

    auto serde_fields() {
        return std::tie(_crash_time, _crash_reason, _stacktrace);
    }
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

} // namespace crash_tracker
