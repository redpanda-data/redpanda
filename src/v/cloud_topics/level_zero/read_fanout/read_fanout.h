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

#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"

namespace cloud_topics::l0 {

/// Read fanout stage coverts vectorized read requests into multiple
/// parallel requests. The client is allowed to send wide read requests
/// that target multiple extents that may be stored in different L0
/// objects. The read fanout stage splits the requests into multiple
/// smaller requests that are then processed in parallel by the
/// fetch request handler stage. The fetch handler can't run individual
/// requests in parallel due to its own limitations. This stage allows
/// to regain some parallelism.
///
/// For requests that target single extent the stage simply forwards
/// them to the next stage without any modifications.
class read_fanout : public read_pipeline_actor<> {
    using actor_t = read_pipeline_actor<>;

public:
    explicit read_fanout(l0::read_pipeline<>::stage);

    struct stats {
        size_t requests_in{0};
        size_t requests_out{0};
        size_t requests_fail{0};
    };

    stats get_stats() const noexcept;

protected:
    ss::future<> process(pipeline_notification msg) override;
    void on_error(std::exception_ptr e) noexcept override;

private:
    ss::future<> process_single_request(l0::read_request<>* req);

    stats _stats;
};
} // namespace cloud_topics::l0
