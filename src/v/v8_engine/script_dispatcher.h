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
#include "outcome.h"
#include "v8_engine/errc.h"
#include "v8_engine/logger.h"
#include "v8_engine/script.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <atomic>
#include <exception>
#include <iostream>
#include <variant>

namespace v8_engine {

struct script_compile_params {
    model::topic_namespace topic;
    ss::sstring function_name;
    iobuf code;
    size_t max_heap_size_bytes{10000};
    size_t run_timeout_ms{200};

    script_compile_params(
      model::topic_namespace topic, ss::sstring function_name, iobuf code)
      : topic(std::move(topic))
      , function_name(std::move(function_name))
      , code(std::move(code)) {}

    script_compile_params(const script_compile_params& other)
      : topic(other.topic)
      , function_name(other.function_name)
      , code(other.code.copy())
      , max_heap_size_bytes(other.max_heap_size_bytes)
      , run_timeout_ms(other.run_timeout_ms) {}
};

// This class implement database for v8_runtimes. It can be updated by
// data_policy_manager. Fetch request will only run get for getting v8_runtimes

// clang-format off
template<typename Executor>
CONCEPT(requires requires(Executor executor) {
    { executor.submit() } -> ss::future<>;
})
// clang-format on
class script_dispatcher {
    constexpr static size_t _mutex_count{50};

public:
    explicit script_dispatcher(Executor& executor)
      : _executor(executor) {}

    ss::future<> start() {
        (void)ss::with_gate(_gate, [this] {
            return ss::do_until(
              [this] { return _as.abort_requested(); },
              [this]() -> ss::future<> { co_await update_runtimes(); });
        });

        co_return;
    }

    ss::future<> stop() {
        _as.request_abort();
        co_await _gate.close();
    }

    // Insert only put compilation params to map.
    bool insert(
      model::topic_namespace topic, ss::sstring function_name, iobuf code) {
        return _compilation_params
          .emplace(
            topic,
            script_compile_params(
              topic, std::move(function_name), std::move(code)))
          .second;
    }

    // Will delete information about dp from compilation_params map, Also delete
    // runtime if it exists.
    // If we change data_policy several times during compilation, we
    // need to notify background workwe that his runtimes expired and background
    // worker should not save this runtimes to map, because after that runtimes
    // map will contain "old" v8.
    bool remove(const model::topic_namespace& topic) {
        bool res = _compilation_params.erase(topic) == 1;
        if (!res) {
            return res;
        }

        auto it = _runtimes.find(topic);
        if (it == _runtimes.end()) {
            return res;
        }

        ss::visit(
          it->second,
          [](runtimes_counter& counter) {
              counter.is_compiled = false;
              counter.deletion++;
          },
          [this, &it](const ss::lw_shared_ptr<script>&) {
              _runtimes.erase(it);
          });

        return res;
    }

    /// This method return v8_runtimes for data-policy (aka topic on curent
    /// moment). If task inside background queue it will return error.
    result<ss::lw_shared_ptr<script>, errc>
    get(const model::topic_namespace& topic) {
        auto it = _compilation_params.find(topic);
        if (it == _compilation_params.end()) {
            return errc::v8_runtimes_not_exist;
        }

        auto runtimes_it = _runtimes.find(topic);
        if (runtimes_it == _runtimes.end()) {
            // Need to compile runtimes at first time.
            auto [_, res] = _runtimes.emplace(topic, runtimes_counter());
            _event_queue.emplace_back(it->second);
            vassert(
              res, "Can not update _runtimes database for topic{}", topic);
            return errc::v8_runtimes_compiling;
        }

        return ss::visit(
          runtimes_it->second,
          [it, this](runtimes_counter& counter)
            -> result<ss::lw_shared_ptr<script>, errc> {
              if (!counter.is_compiled) {
                  // Need to add task to compile v8_runtimes, because user
                  // remove old data_policy and insert new dp for current topic
                  counter.is_compiled = true;
                  _event_queue.emplace_back(it->second);
              }
              return errc::v8_runtimes_compiling;
          },
          [](ss::lw_shared_ptr<script> script_ptr)
            -> result<ss::lw_shared_ptr<script>, errc> { return script_ptr; });
    }

private:
    // This method understand did user update data_policy for topic and we need
    // to skip some compilation tasks
    bool is_data_policy_changed(const model::topic_namespace& topic) {
        auto runtimes_it = _runtimes.find(topic);
        vassert(
          runtimes_it != _runtimes.end(),
          "On compilation moment runtimes database must contain info about "
          "waiting runtimes request for topic({})",
          topic);
        return ss::visit(
          runtimes_it->second,
          [&runtimes_it, this](runtimes_counter& counter) -> bool {
              // Need to skip this task;
              if (counter.deletion > 0) {
                  // We handle all compilation task and skip all of them.
                  if (--counter.deletion == 0) {
                      _runtimes.erase(runtimes_it);
                  }
                  return true;
              }
              return false;
          },
          [topic](const ss::lw_shared_ptr<script>&) -> bool {
              vassert(
                false,
                "Task queue for update runtimes can not contain task if "
                "runtime exists for topic({})",
                topic);
              return false;
          });
    }

    // Internal function which read updates from queue and change hash_map with
    // v8_runtimes.
    ss::future<> update_runtimes() {
        while (!_event_queue.empty()) {
            auto& compilation_event = _event_queue.front();

            // check before compilatioin do we need skip this task.
            if (is_data_policy_changed(compilation_event.topic)) {
                _event_queue.pop_front();
                co_return;
            }

            auto script_ptr = ss::make_lw_shared<script>(
              compilation_event.max_heap_size_bytes,
              compilation_event.run_timeout_ms);

            bool is_compiled = true;
            try {
                co_await script_ptr->init(
                  compilation_event.function_name,
                  std::move(compilation_event.code),
                  _executor);
            } catch (const std::exception& e) {
                log.error(
                  "Can not compile data_policy funtion({}) for topic({}). "
                  "Error: ",
                  compilation_event.function_name,
                  compilation_event.topic,
                  e.what());
                is_compiled = false;
            }

            if (is_data_policy_changed(compilation_event.topic)) {
                _event_queue.pop_front();
                co_return;
            }

            if (is_compiled) {
                _runtimes.insert_or_assign(compilation_event.topic, script_ptr);
            } else {
                vassert(
                  _runtimes.erase(compilation_event.topic) == 0,
                  "Inconsistency script_dispatcher on failed compilation. Can "
                  "not find info about waiting runtime request for topic({})",
                  compilation_event.topic);
            }
            _event_queue.pop_front();
        }
    }

private:
    absl::node_hash_map<model::topic_namespace, script_compile_params>
      _compilation_params;

    // Queue with updates for script_dispatcher;
    std::list<script_compile_params> _event_queue;

    // This struct can help to understand do we need to sckip compilation,
    // because data_policy was updated. Scenario looks like [insert dp1] ->
    // [get(push compilation task to queue)] -> [remove dp 1] -> [insert dp2] ->
    // [get(we need to cancel first compilation, because user can get expared
    // state)]
    struct runtimes_counter {
        bool is_compiled{true};
        int64_t deletion{0};
    };

    using runtimes_db_val
      = std::variant<runtimes_counter, ss::lw_shared_ptr<script>>;

    // Database for v8_runtimes
    absl::node_hash_map<model::topic_namespace, runtimes_db_val> _runtimes;

    Executor& _executor;

    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace v8_engine