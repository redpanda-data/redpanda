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
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics::l0 {

/// Read request handler.
/// This component can process dl_placeholder batches.
/// This component should be split up into separate components in the
/// future (one for materialization step, one for reading from cache,
// etc). Currently everything is done in one place for simplicity.
class fetch_handler {
public:
    explicit fetch_handler(
      l0::read_pipeline<>::stage,
      cloud_storage_clients::bucket_name,
      cloud_io::remote_api<>*,
      cloud_io::basic_cache_service_api<>*);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> bg_process_requests();

    /// Run resolver loop once
    ss::future<checked<bool, errc>> process_requests();

    /// Process single request
    ss::future<> process_single_request(l0::read_request<>* req);

    cloud_storage_clients::bucket_name _bucket;
    cloud_io::remote_api<>* _remote;
    cloud_io::basic_cache_service_api<>* _cache;
    retry_chain_node _rtc;
    retry_chain_logger _logger;
    ss::gate _gate;
    l0::read_pipeline<>::stage _pipeline_stage;
};
} // namespace experimental::cloud_topics::l0
