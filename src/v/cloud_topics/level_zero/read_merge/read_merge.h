/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/level_zero_probe.h"
#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>

#include <optional>

namespace cloud_topics::l0 {

/// Merges concurrent read requests targeting the same L0 object.
///
/// Keys on exact object_id (no hash collisions) and has no fixed
/// timeout — the wait scales to actual download time.
template<class Clock = ss::lowres_clock>
class read_merge : public read_pipeline_actor<Clock> {
    using actor_t = read_pipeline_actor<Clock>;

public:
    explicit read_merge(read_pipeline<Clock>::stage);
    ss::future<> stop() override;

protected:
    ss::future<> process(pipeline_notification msg) override;
    void on_error(std::exception_ptr e) noexcept override;

private:
    using semaphore_units
      = seastar::semaphore_units<ss::named_semaphore_exception_factory, Clock>;
    ss::future<>
    process_single_request(read_request<Clock>* req, semaphore_units u);

    read_merge_probe _probe;

    chunked_hash_map<object_id, ss::shared_promise<std::optional<errc>>>
      _in_flight;

    ssx::named_semaphore<Clock> _in_flight_sem;
};
} // namespace cloud_topics::l0
