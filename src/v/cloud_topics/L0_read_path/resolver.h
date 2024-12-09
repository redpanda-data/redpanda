/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/interfaces/cluster_partition_manager.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics {

/// Read request resolver.
/// This component can only process
class resolver {
public:
    explicit resolver(
      core::read_pipeline<>*,
      cloud_storage_clients::bucket_name,
      cloud_io::remote_api<>*,
      cloud_io::basic_cache_service_api<>*,
      ss::shared_ptr<cluster_partition_manager_api>);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> bg_resolve_pipeline();
    /// Run resolver loop once
    ss::future<checked<bool, errc>> process_requests();

    /// Process single request
    ss::future<> process_single_request(core::read_request<>* req);

    core::read_pipeline<>* _pipeline;
    cloud_storage_clients::bucket_name _bucket;
    cloud_io::remote_api<>* _remote;
    cloud_io::basic_cache_service_api<>* _cache;
    ss::shared_ptr<cluster_partition_manager_api> _pm;
    retry_chain_node _rtc;
    retry_chain_logger _logger;
    ss::gate _gate;
    core::pipeline_stage _my_stage;
};
} // namespace experimental::cloud_topics
