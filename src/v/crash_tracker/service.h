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
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace crash_tracker {

class service {
public:
    using crash_recorder_fn = void (*)(crash_description&);

    ss::future<> start();
    ss::future<> stop();

    // Async-signal safe
    void record_crash(crash_recorder_fn);

private:
    ss::future<ss::sstring> describe_recorded_crashes() const;
    bool update_crash_md(const crash_description&);
    ss::future<> check_for_crash_loop() const;
    ss::future<> initialize_state();

    std::atomic<bool> initialized{false};
    crash_description prepared_cd;
    iobuf serde_output;
    std::filesystem::path crash_report_file_name;
    int fd{0};
};

} // namespace crash_tracker
