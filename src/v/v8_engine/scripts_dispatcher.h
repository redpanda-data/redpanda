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

#include "coproc/event_handler.h"
#include "model/data_policy.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/mutex.h"
#include "v8_engine/executor.h"
#include "v8_engine/script.h"
#include "vassert.h"

#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <optional>

namespace v8_engine {

template<typename Executor>
class script_guard
  : public seastar::weakly_referencable<script_guard<Executor>> {
    static constexpr size_t _max_heap_size_bytes{10000};
    static constexpr size_t _run_timeout_ms{200};

public:
    struct init_param {
        ss::sstring function_name;
        iobuf code;
    };

    explicit script_guard(const model::data_policy& dp, iobuf code) {
        init_param new_script{
          .function_name = dp.function_name, .code = std::move(code)};
        _data = std::move(new_script);
    }

    ss::future<> run(model::record_batch& batch, Executor& executor) {
        auto lock = co_await _mtx.get_units();
        co_await ss::visit(
          _data,
          [&executor, &batch, this](init_param& params) {
              init_param save_params = std::move(params);
              script new_script(_max_heap_size_bytes, _run_timeout_ms);
              this->_data = std::move(new_script);
              auto& sc = std::get<script>(this->_data);
              return sc
                .init(
                  save_params.function_name,
                  std::move(save_params.code),
                  executor)
                .then([&executor, &batch, this] {
                    auto& sc = std::get<script>(this->_data);
                    return sc.run(batch, executor);
                });
          },
          [&executor, &batch](script& sc) { return sc.run(batch, executor); });
    }

private:
    mutex _mtx;
    std::variant<init_param, script> _data;
};

template<typename Executor>
struct script_with_env_wrapper {
    bool contains_script() { return script.get() == nullptr ? false : true; }

    ss::lw_shared_ptr<script_guard<Executor>> script;
    Executor& executor;
};

template<typename Executor>
class scripts_dispatcher {
public:
    explicit scripts_dispatcher(
      Executor& executor,
      coproc::wasm::data_policy_event_handler& code_database)
      : _executor(executor)
      , _code_database(code_database) {}

    ss::future<script_with_env_wrapper<Executor>>
    get_script(const model::topic_namespace& topic) {
        auto res = script_with_env_wrapper<Executor>{
          .script = nullptr, .executor = _executor};

        auto it = _scripts.find(topic);
        if (it == _scripts.end()) {
            co_return res;
        }

        res.script = it->second;
        co_return res;
    }

    void update_script(
      const model::topic_namespace& topic, model::data_policy& data_policy) {
        _scripts.insert_or_assign(topic, data_policy);
    }

    void delete_script(const model::topic_namespace& topic) {
        _scripts.erase(topic);
    }

private:
    iobuf get_code(model::data_policy& data_policy) {
        auto code = _code_database.get_code(data_policy.script_name);

        if (!code.has_value()) {
            throw std::runtime_error(
              fmt::format("Can not find code for data_policy {}", data_policy));
        }

        return std::move(code.value());
    }

    absl::node_hash_map<
      model::topic_namespace,
      ss::lw_shared_ptr<script_guard<Executor>>>
      _scripts;
    Executor& _executor;
    coproc::wasm::data_policy_event_handler& _code_database;
};

using scripts_table = scripts_dispatcher<executor_wrapper>;

} // namespace v8_engine
