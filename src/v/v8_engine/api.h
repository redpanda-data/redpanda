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

#include "v8_engine/data_policy_table.h"
#include "v8_engine/executor.h"
#include "v8_engine/script_dispatcher.h"

namespace v8_engine {

// Unit for work with v8_engine stuff
class api {
public:
    explicit api(executor_service& executor)
      : _executor(executor)
      , _script_dispatcher(_dp_table, executor) {}

    ss::future<> insert_code(coproc::script_id id, iobuf code);
    ss::future<> erase_code(coproc::script_id id);

private:
    executor_service& _executor;
    data_policy_table _dp_table;
    script_dispatcher<executor_service> _script_dispatcher;
};

} // namespace v8_engine
