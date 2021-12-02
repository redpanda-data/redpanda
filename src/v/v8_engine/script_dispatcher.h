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

#include "bytes/iobuf.h"
#include "model/metadata.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <v8_engine/script.h>

namespace v8_engine {

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

template<typename Executor>
class script_dispatcher {
public:
    explicit script_dispatcher(Executor& executor)
      : _executor(executor) {}

    bool insert(
      const model::topic_namespace& topic,
      ss::lw_shared_ptr<script_wrpapper> new_script) {
        return _scripts.emplace(topic, new_script).second;
    }

    void remove(const model::topic_namespace& topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return;
        }
        it->second->destroy();
        _scripts.erase(it);
    }

    bool contains(const model::topic_namespace& topic) {
        return _scripts.contains(topic);
    }

    // Get reqeust use lazy compilation for v8_script.
    ss::future<ss::lw_shared_ptr<script>> get(model::topic_namespace topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            co_return nullptr;
        }

        if (it->second->is_compiling()) {
            // If script is being compiled now we need to wait finish
            co_await it->second->cv.wait();
        }

        // If user delete script, function will contain nullptr
        co_return it->second->function;
    }

    // Compare pointers and check if script was deleted
    bool script_is_deleted(
      const model::topic_namespace& topic,
      ss::lw_shared_ptr<script_wrpapper> value) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return true;
        }

        return value.get() != it->second.get();
    }

private:
    // Hash map from topic to scripts(v8 runtimes). It stores v8_script for
    // fetch requests
    absl::
      node_hash_map<model::topic_namespace, ss::lw_shared_ptr<script_wrpapper>>
        _scripts;

    Executor& _executor;
};

} // namespace v8_engine
