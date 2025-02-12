/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/deps.h"

namespace datalake::translation {

ss::future<> noop_mem_tracker::maybe_reserve_memory(size_t, ss::abort_source&) {
    return ss::make_ready_future<>();
}
void noop_mem_tracker::update_current_memory_usage(size_t) {}
void noop_mem_tracker::release() {}

} // namespace datalake::translation
