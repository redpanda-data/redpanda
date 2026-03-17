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

#include "base/seastarx.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/remote.h"
#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l0 {

/// Read request handler.
/// This component can process ctp_placeholder batches.
/// This component should be split up into separate components in the
/// future (one for materialization step, one for reading from cache,
// etc). Currently everything is done in one place for simplicity.
class fetch_handler : public read_pipeline_actor<> {
    using actor_t = read_pipeline_actor<>;

public:
    explicit fetch_handler(
      l0::read_pipeline<>::stage,
      cloud_storage_clients::bucket_name,
      cloud_io::remote_api<>*,
      cloud_io::basic_cache_service_api<>*);

protected:
    ss::future<> process(pipeline_notification msg) override;
    void on_error(std::exception_ptr e) noexcept override;

private:
    /// Process single request
    ss::future<> process_single_request(l0::read_request<>* req);

    cloud_storage_clients::bucket_name _bucket;
    cloud_io::remote_api<>* _remote;
    cloud_io::basic_cache_service_api<>* _cache;
    retry_chain_node _rtc;
    retry_chain_logger _logger;
};
} // namespace cloud_topics::l0
