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
#include "v8_engine/script_dispatcher.h"

namespace v8_engine {

class api {
public:
    api() = default;

    ss::future<> start(ss::alien::instance& instance);

    ss::future<> stop();

    ss::future<> insert_code(coproc::script_id id, iobuf code);
    ss::future<> erase_code(coproc::script_id id);

private:
    bool is_enabled();

private:
    std::optional<enviroment> _env;
    executor_service _executor;
    ss::sharded<script_dispatcher<executor_service>> _script_dispatcher;
};

} // namespace v8_engine
