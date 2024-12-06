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

#include "crash_tracker/limiter.h"
#include "crash_tracker/recorder.h"

#include <exception>
#include <memory>

namespace crash_tracker {

class service {
public:
    service() noexcept;

    /// Should be called on startup before any call to record_crash_*, otherwise
    /// record_crash_* is a noop.
    ss::future<> start();
    ss::future<> stop();

    recorder& get_recorder() { return crash_tracker::get_recorder(); }
    limiter& get_limiter() { return _limiter; }

private:
    limiter _limiter;
};

} // namespace crash_tracker
