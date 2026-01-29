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

#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/types.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>

namespace cloud_topics::l0 {

template<class Clock = ss::lowres_clock>
class read_debounce {
public:
    explicit read_debounce(read_pipeline<Clock>::stage);
    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> bg_loop();
    ss::future<> process_single_request(read_request<Clock>* req);

    ss::gate _gate;
    read_pipeline<Clock>::stage _pipeline_stage;

    struct debounce_state {
        ssx::basic_checkpoint_mutex<Clock> lock{"read_debounce"};
    };
    static constexpr size_t debounce_hash_size = 128;
    std::array<debounce_state, debounce_hash_size> _in_flight{};
};
} // namespace cloud_topics::l0
