/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/data_writer_interface.h"

namespace datalake::translation {

class noop_mem_tracker : public writer_mem_tracker {
public:
    ss::future<> maybe_reserve_memory(size_t bytes, ss::abort_source&) override;
    void update_current_memory_usage(size_t) override;
    void release() override;
};

} // namespace datalake::translation
