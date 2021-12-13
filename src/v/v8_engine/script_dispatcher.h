
/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/metadata.h"
#include "v8_engine/data_policy_table.h"
#include "v8_engine/logger.h"
#include "v8_engine/script.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

namespace v8_engine {

// This class implement logic to store v8_engine::script.
template<typename Executor>
class script_dispatcher {
    static constexpr size_t max_heap_size_bytes = 10000;
    static constexpr size_t run_timeout_ms = 10000;

public:
    script_dispatcher(data_policy_table& dp_table, Executor& executor)
      : _dp_table(dp_table)
      , _executor(executor) {}

    // This method will be ran from fetch request. We need to understand do we
    // need to compile script or only need to wait compilation and return
    // pointer to script
    ss::future<ss::lw_shared_ptr<script>> get(model::topic_namespace topic) {
        auto it = _scripts.find(topic);
        if (it != _scripts.end()) {
            co_return co_await get_internal(it->second);
        }

        co_return co_await compile(topic);
    }

    // Only delete script from hash_map and notify waiters about end of
    // compilation
    void remove(const model::topic_namespace& topic) {
        auto it = _scripts.find(topic);
        if (it != _scripts.end()) {
            // We need to destory script to signal waiters about removing
            it->second->destroy();
            _scripts.erase(it);
        }
    }

private:
    struct script_wrpapper {
        ss::condition_variable cv;
        ss::lw_shared_ptr<script> function{nullptr};

        bool is_compiling() { return function.get() == nullptr; }

        // TODO: add stop compilation (Is it possible?))
        void destroy() {
            function = nullptr;
            cv.broadcast();
        }
    };

    // Data-policy was deleted if hash_map does not contain script for topic or
    // pointer from hash_map and our ptr are different
    bool script_is_deleted(
      const model::topic_namespace& topic,
      ss::lw_shared_ptr<script_wrpapper> script_ptr) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return true;
        }

        return it->second != script_ptr;
    }

    ss::future<ss::lw_shared_ptr<script>>
    get_internal(ss::lw_shared_ptr<script_wrpapper> script_ptr) {
        if (script_ptr->is_compiling()) {
            co_await script_ptr->cv.wait();
        }

        // If user delete script it returns nullptr
        co_return script_ptr->function;
    }

    ss::future<ss::lw_shared_ptr<script>>
    compile(model::topic_namespace topic) {
        auto dp = _dp_table.get_data_policy(topic);
        if (!dp.has_value()) {
            co_return nullptr;
        }

        // Insert information that compilation is starting. After that all
        // changes for data_policy will be visable. Because we put conditional
        // variable to script_dispatcher. We can not do any async request
        // between find for _script and insert.
        auto new_script_wrapper = ss::make_lw_shared<script_wrpapper>();
        auto [_, res] = _scripts.emplace(topic, new_script_wrapper);
        vassert(
          res,
          "Can not insert new script wrapper to script_dispatcher for "
          "topoic({})",
          topic);

        auto code = co_await _executor.get_code(dp.value().script_name());
        if (!code.has_value()) {
            vlog(
              logger.error,
              "Can not find js code for dp({}) for topic({})",
              dp.value(),
              topic);
            _scripts.erase(topic);
            co_return nullptr;
        }

        auto script_ptr = ss::make_lw_shared<script>(
          max_heap_size_bytes, run_timeout_ms);

        try {
            co_await script_ptr->init(
              dp.value().function_name(), std::move(code.value()), _executor);
        } catch (const v8_engine::script_exception& ex) {
            vlog(
              logger.error,
              "Error in v8 compilation for dp({}) for topic({}). Error: {}",
              dp.value(),
              topic,
              ex.what());
            if (!script_is_deleted(topic, new_script_wrapper)) {
                // Delete information about compilation
                _scripts.erase(topic);
            }
            co_return nullptr;
        };

        if (script_is_deleted(topic, new_script_wrapper)) {
            co_return nullptr;
        }

        new_script_wrapper->function = script_ptr;
        new_script_wrapper->cv.broadcast();

        co_return script_ptr;
    }

private:
    // Hash map from topic to scripts(v8 runtimes). It store compilation
    // params or shared_ptr to script_wrpapper.
    absl::
      node_hash_map<model::topic_namespace, ss::lw_shared_ptr<script_wrpapper>>
        _scripts;

    // External services to get compilation information
    data_policy_table& _dp_table;
    Executor& _executor;
};

} // namespace v8_engine