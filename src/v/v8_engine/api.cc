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

#include "cluster/errc.h"
#include "config/configuration.h"
#include "syschecks/syschecks.h"

#include <seastar/core/shared_ptr.hh>

#include <cstddef>

namespace v8_engine {

api::api()
  : _data_policy_event_handler(_executor) {}

ss::future<> api::start(
  ss::alien::instance& instance, coproc::wasm::event_listener* event_listener) {
    if (is_enabled()) {
        co_await syschecks::systemd_message("Creating v8_engine::api");
        co_await _executor.start(
          instance, config::shard_local_cfg().executor_queue_size());
        co_await _dp_table.start();
        co_await _script_dispatcher.start(std::ref(_executor));

        event_listener->register_handler(
          coproc::wasm::event_type::data_policy, &_data_policy_event_handler);
    }
}

ss::future<> api::stop() {
    if (is_enabled()) {
        co_await _script_dispatcher.stop();
        co_await _dp_table.stop();
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

ss::future<std::error_code>
api::insert(model::topic_namespace topic, data_policy dp) {
    if (!is_enabled()) {
        co_return std::error_code(cluster::errc::data_policy_not_enabled);
    }

    if (!_dp_table.local().insert(topic, dp)) {
        co_return std::error_code(cluster::errc::data_policy_already_exists);
    }

    co_return std::error_code(cluster::errc::success);
}

std::error_code api::remove(const model::topic_namespace& topic) {
    if (!is_enabled()) {
        return std::error_code(cluster::errc::data_policy_not_enabled);
    }

    if (!_dp_table.local().erase(topic)) {
        return std::error_code(cluster::errc::data_policy_not_exists);
    }

    _script_dispatcher.local().remove(topic);

    return std::error_code(cluster::errc::success);
}

std::optional<data_policy> api::get_dp(const model::topic_namespace& topic) {
    if (!is_enabled()) {
        return std::nullopt;
    }

    return _dp_table.local().get_data_policy(topic);
}

ss::future<ss::lw_shared_ptr<script>>
api::get_script(model::topic_namespace topic) {
    if (!is_enabled()) {
        co_return nullptr;
    }

    // I need to check is script_dispatcher contains dp or not.
    if (_script_dispatcher.local().contains(topic)) {
        co_return co_await _script_dispatcher.local().get(topic);
    }

    // Find data_policy information
    auto dp = get_dp(topic);
    if (!dp.has_value()) {
        co_return nullptr;
    }

    // Inserat inforamtion that compilation is starting to script_dispatcher
    // After that all changes for data_policy will be visable. Because we put
    // conditional variable to script_dispatcher. We can not do any async
    // request between contains and insert.
    auto new_script_wrapper = ss::make_lw_shared<script_wrpapper>();
    vassert(
      _script_dispatcher.local().insert(topic, new_script_wrapper),
      "Can not insert new script wrapper to script_dispatcher for topoic({})",
      topic);

    auto code = co_await _executor.get_code(dp.value().script_name());
    if (!code.has_value()) {
        // TODO: add logging
        // Need to remove information about compilation form script_dispatcher.
        // User can recompile it again.
        _script_dispatcher.local().remove(topic);
        co_return nullptr;
    }

    auto script_ptr = ss::make_lw_shared<script>(
      _max_heap_size_bytes, _run_timeout_ms);

    try {
        co_await script_ptr->init(
          dp.value().function_name(), std::move(code.value()), _executor);
    } catch (const v8_engine::script_exception& ex) {
        // add log
        if (!_script_dispatcher.local().script_is_deleted(
              topic, new_script_wrapper)) {
            // Delete information about compilation
            _script_dispatcher.local().remove(topic);
        }

        co_return nullptr;
    }

    if (_script_dispatcher.local().script_is_deleted(
          topic, new_script_wrapper)) {
        co_return nullptr;
    }

    new_script_wrapper->function = script_ptr;
    new_script_wrapper->cv.broadcast();

    co_return script_ptr;
}

} // namespace v8_engine
