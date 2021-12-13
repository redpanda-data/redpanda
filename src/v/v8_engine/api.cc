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

ss::future<> api::insert_code(coproc::script_id id, iobuf code) {
    co_await _executor.insert_or_assign(id, std::move(code));
}

ss::future<> api::erase_code(coproc::script_id id) {
    co_await _executor.erase(id);
}

} // namespace v8_engine
