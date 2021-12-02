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

    // Insert only put compilation params to map. It can not rewrite
    // key, because we need to run remove for delete old v8_script and insert
    // new
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

    // Remove check do user run get and start to compile script. After that it
    // delete all information about v8_script from hash_map
    bool remove(const model::topic_namespace& topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return false;
        }

        ss::visit(
          it->second,
          [](const script_compile_params&) {
              // Do nothing, because nobody try to get this script. User only
              // insert data_policy for topic and did not try t ouse it
              return;
          },
          [](ss::lw_shared_ptr<script_wrpapper> script_ptr) {
              // If user started to compile script we need to notify waters that
              // we delete this data_policy, after that we can clear hash_table
              if (script_ptr->is_compiling()) {
                  script_ptr->destroy();
              }
              return;
          });

        _scripts.erase(it);

        return true;
    }

    // Get reqeust use lazy compilation for v8_script. When user run get first
    // time for data_policy we will compile script and anotjer get request for
    // cirrent topic will wait finish of compilation
    ss::future<ss::lw_shared_ptr<script>> get(model::topic_namespace topic) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            co_return nullptr;
        }

        co_return co_await ss::visit(
          it->second,
          // If we have only compilation params we need to compile script
          [topic, this](script_compile_params& params)
            -> ss::future<ss::lw_shared_ptr<script>> {
              auto topic_name = topic;
              script_compile_params copy_params = std::move(params);
              auto value = ss::make_lw_shared(script_wrpapper());

              // Can skip result, because it contains compilation_params and
              // nobody change this map since find.
              _scripts.insert_or_assign(topic_name, value);

              auto runtime = ss::make_lw_shared<v8_engine::script>(
                copy_params.max_heap_size_bytes, copy_params.run_timeout_ms);

              try {
                  co_await runtime->init(
                    copy_params.function_name,
                    std::move(copy_params.code.copy()),
                    _executor);
              } catch (const v8_engine::script_exception& ex) {
                  // TODO: add log

                  // If script is exist we need to return compilation params
                  // back, becuase use want to try recompile it
                  if (!script_is_deleted(topic_name, value)) {
                      _scripts.insert_or_assign(
                        topic_name, std::move(copy_params));
                      value->destroy();
                  }

                  // If script was deleted we do not do anything, because remove
                  // method destroy it.
                  co_return nullptr;
              }

              // Skip runtime is script was deleted
              if (script_is_deleted(topic_name, value)) {
                  co_return nullptr;
              }
              value->function = runtime;
              value->cv.broadcast();
              co_return runtime;
          },
          // If we see ptr to v8_script we need wait compilation or return
          // result
          [](ss::lw_shared_ptr<script_wrpapper> script_ptr)
            -> ss::future<ss::lw_shared_ptr<script>> {
              if (script_ptr->is_compiling()) {
                  // If script is being compiled now we need to wait finish
                  co_await script_ptr->cv.wait();
                  // If user delete script, function will contain nullptr
                  co_return script_ptr->function;
              } else {
                  // Return value (v8_script) for current topic
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

    // Compare pointers and check if script was deleted
    bool script_is_deleted(
      const model::topic_namespace& topic,
      ss::lw_shared_ptr<script_wrpapper> script_ptr) {
        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            return true;
        }

        return ss::visit(
          it->second,
          // User delete script and set new compilation params
          [](const script_compile_params&) { return true; },
          // User can compile new script and put it to hash table, we need to
          // check pointers
          [script_ptr](ss::lw_shared_ptr<script_wrpapper> new_script_ptr) {
              return script_ptr.get() != new_script_ptr.get();
          });
    }

    using scripts_value
      = std::variant<script_compile_params, ss::lw_shared_ptr<script_wrpapper>>;

    // Hash map from topic to scripts(v8 runtimes). It store compilation params
    // or shared_ptr to script_wrpapper. When user set data_policy by using
    // incremental topic config we check does code exist and put information
    // about compilation to hash map.
    absl::node_hash_map<model::topic_namespace, scripts_value> _scripts;

    Executor& _executor;
};

} // namespace v8_engine
