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

#include "crash_tracker/service.h"

#include "config/node_config.h"
#include "crash_tracker/limiter.h"
#include "crash_tracker/recorder.h"

#include <seastar/core/seastar.hh>

namespace crash_tracker {

service::service() noexcept
  : _limiter(get_recorder()) {}

ss::future<> service::start() {
    if (config::node().developer_mode()) {
        // crash loop tracking has value only in long running clusters
        // that can potentially accumulate state across restarts.
        co_return;
    }

    co_await _limiter.check_for_crash_loop();
    co_await get_recorder().start();
}

ss::future<> service::stop() {
    co_await _limiter.record_clean_shutdown();
    co_await get_recorder().stop();
}

} // namespace crash_tracker
