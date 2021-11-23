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

#include "v8_engine/environment.h"
#include "v8_engine/executor.h"

#include <seastar/core/sharded.hh>

namespace v8_engine {

// This class implements wrapper for executor.
// In current design executor exist on one core
// And for submit tasks you need to run invoke_on
// So this class wrap this logic for v8_engine::script
class executor_service {
    static constexpr size_t _home_core = 0;
    static constexpr unsigned _core_for_std_thread = 0;

public:
    executor_service(ss::alien::instance& instance, int64_t size);

    ss::future<> start();
    ss::future<> stop();

    // We do not need use gate there, because executor has gate inside submit
    // function
    template<typename WrapperFuncForExecutor>
    ss::future<> submit(
      WrapperFuncForExecutor&& func_for_executor,
      std::chrono::milliseconds timeout) {
        return _executor.invoke_on(
          _home_core,
          [func_for_executor = std::forward<WrapperFuncForExecutor>(
             func_for_executor),
           timeout](executor& exec) mutable {
              return exec.submit(
                std::forward<WrapperFuncForExecutor>(func_for_executor),
                timeout);
          });
    }

private:
    ss::alien::instance& _instance;
    size_t _size;
    ss::sharded<executor> _executor;
};

class api {
public:
    api(ss::alien::instance& instance, int64_t size);

    ss::future<> start();
    ss::future<> stop();

    executor_service& get_executor();

private:
    enviroment _env;
    executor_service _executor_service;
};

} // namespace v8_engine
