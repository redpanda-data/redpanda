/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/api.h"

#include "config/configuration.h"
#include "syschecks/syschecks.h"

namespace v8_engine {

ss::future<> api::start(ss::alien::instance& instance) {
    if (is_enabled()) {
        co_await syschecks::systemd_message("Creating v8_engine::api");
        co_await _executor.start(
          instance, config::shard_local_cfg().executor_queue_size());
        co_await _script_dispatcher.start(std::ref(_executor));
    }
}

ss::future<> api::stop() {
    if (is_enabled()) {
        co_await _script_dispatcher.stop();
        co_await _executor.stop();
    }
}

ss::future<> api::insert_code(coproc::script_id id, iobuf code) {
    if (!is_enabled()) {
        co_return;
    }
    co_await _executor.insert_or_assign(id, std::move(code));
}

ss::future<> api::erase_code(coproc::script_id id) {
    if (!is_enabled()) {
        co_return;
    }
    co_await _executor.erase(id);
}

bool api::is_enabled() {
    auto& local_cfg = config::shard_local_cfg();
    return local_cfg.enable_v8() && local_cfg.developer_mode();
}

} // namespace v8_engine
