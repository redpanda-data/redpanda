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

struct script_compile_params {
    script_compile_params(ss::sstring function_name, iobuf code)
      : function_name(std::move(function_name))
      , code(std::move(code)) {}

    ss::sstring function_name;
    iobuf code;
    size_t max_heap_size_bytes{10000};
    size_t run_timeout_ms{10000};
};

template<typename Executor>
class script_dispatcher {
public:
    explicit script_dispatcher(Executor& executor)
      : _executor(executor) {}

    // Insert only put compilation params to map. It must not contain value for
    // topic.
    bool insert(
      const model::topic_namespace& topic,
      ss::sstring function_name,
      iobuf code) {
        return _scripts
          .emplace(
            topic,
            script_compile_params(std::move(function_name), std::move(code)))
          .second;
    }

    // Remove only deletes script if it has been compiled or it notify waiters
    // that script is removed
    bool remove(const model::topic_namespace& topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return false;
        }

        ss::visit(
          it->second,
          [](const script_compile_params&) {
              // Do nothing, because nobody try to get this script
              return;
          },
          [](ss::lw_shared_ptr<script_wrpapper> script_ptr) {
              if (script_ptr->is_compiling()) {
                  script_ptr->destroy();
              }
              return;
          });

        _scripts.erase(it);

        return true;
    }

    ss::future<ss::lw_shared_ptr<script>> get(model::topic_namespace topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            co_return nullptr;
        }

        co_return co_await ss::visit(
          it->second,
          [topic, this](script_compile_params& params)
            -> ss::future<ss::lw_shared_ptr<script>> {
              auto topic_name = topic;
              script_compile_params copy_params = std::move(params);
              auto value = ss::make_lw_shared(script_wrpapper());

              // Can skip result, because it contains compilation_params
              _scripts.insert_or_assign(topic_name, value);

              auto runtime = ss::make_lw_shared<v8_engine::script>(
                copy_params.max_heap_size_bytes, copy_params.run_timeout_ms);

              try {
                  co_await runtime->init(
                    copy_params.function_name,
                    std::move(copy_params.code.copy()),
                    _executor);
              } catch (const v8_engine::script_exception& ex) {
                  // add log
                  if (!script_is_deleted(topic_name, value)) {
                      // Need to set compilation_params back, because user can
                      // try to recompile script
                      _scripts.insert_or_assign(
                        topic_name, std::move(copy_params));
                      value->destroy();
                  }
                  co_return nullptr;
              }

              if (script_is_deleted(topic_name, value)) {
                  co_return nullptr;
              }

              value->function = runtime;
              value->cv.broadcast();
              co_return runtime;
          },
          [](ss::lw_shared_ptr<script_wrpapper> script_ptr)
            -> ss::future<ss::lw_shared_ptr<script>> {
              if (script_ptr->is_compiling()) {
                  co_await script_ptr->cv.wait();
                  // If user delete script, function will contain nullptr
                  co_return script_ptr->function;
              } else {
                  co_return script_ptr->function;
              }
          });
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

    bool script_is_deleted(
      const model::topic_namespace& topic,
      ss::lw_shared_ptr<script_wrpapper> script_ptr) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return true;
        }

        return ss::visit(
          it->second,
          [](const script_compile_params&) {
              // User delete script and set new compilation params
              return true;
          },
          [script_ptr](ss::lw_shared_ptr<script_wrpapper> new_script_ptr) {
              return script_ptr.get() != new_script_ptr.get();
          });
    }

    using scripts_value
      = std::variant<script_compile_params, ss::lw_shared_ptr<script_wrpapper>>;

    // Hash map from topic to scripts(v8 runtimes). It store compilation params
    // or shared_ptr to script_wrpapper.
    absl::node_hash_map<model::topic_namespace, scripts_value> _scripts;

    Executor& _executor;
};

} // namespace v8_engine
