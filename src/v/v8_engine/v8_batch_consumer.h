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

#include "kafka/protocol/batch_consumer.h"
#include "model/record.h"
#include "v8_engine/executor.h"
#include "v8_engine/scripts_dispatcher.h"

#include <seastar/core/loop.hh>

namespace v8_engine {

template<typename Executor>
class v8_batch_consumer {
public:
    explicit v8_batch_consumer(
      script_with_env_wrapper<Executor> scripts_with_env)
      : _scripts_with_env(scripts_with_env){};

    v8_batch_consumer(const v8_batch_consumer& o) = delete;
    v8_batch_consumer& operator=(const v8_batch_consumer& o) = delete;
    v8_batch_consumer& operator=(v8_batch_consumer&& o) noexcept = default;
    v8_batch_consumer(v8_batch_consumer&& o) noexcept = default;
    ~v8_batch_consumer() = default;

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        co_await _scripts_with_env.script->run(
          batch, _scripts_with_env.executor);
        _transformed_batches.emplace_back(std::move(batch));
        co_return ss::stop_iteration::no;
    }

    model::record_batch_reader end_of_stream() {
        return model::make_memory_record_batch_reader(
          std::move(_transformed_batches));
    }

private:
    model::record_batch_reader::data_t _transformed_batches;
    script_with_env_wrapper<Executor> _scripts_with_env;
};

} // namespace v8_engine
