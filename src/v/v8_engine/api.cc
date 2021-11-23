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

namespace v8_engine {

executor_service::executor_service(ss::alien::instance& instance, int64_t size)
  : _instance(instance)
  , _size(size) {}

ss::future<> executor_service::start() {
    return _executor.start_single(
      std::ref(_instance), _core_for_std_thread, _size);
}

ss::future<> executor_service::stop() { return _executor.stop(); }

api::api(ss::alien::instance& instance, int64_t size)
  : _executor_service(instance, size) {}

ss::future<> api::start() { return _executor_service.start(); }

ss::future<> api::stop() { return _executor_service.stop(); }

executor_service& api::get_executor() { return _executor_service; }

} // namespace v8_engine
