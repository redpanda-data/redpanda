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

#include "absl/container/btree_map.h"
#include "base/outcome.h"
#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"
#include "cloud_topics/cluster_services.h"
#include "cloud_topics/level_zero/cluster_services_impl/cluster_services.h"
#include "cloud_topics/level_zero/common/level_zero_probe.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/types.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace cloud_io {
template<typename Clock>
class remote_api;
class remote;
} // namespace cloud_io

namespace cloud_topics::l0 {

struct batcher_result {
    uuid_t uuid;
    // Reader that contains placeholder batches. Batches
    // should map to original batches 1:1 but have different
    // content.
    std::unique_ptr<model::record_batch_reader> reader;
};

struct batcher_accessor;

/// The data path uploader
///
/// The batcher collects a list of write_request instances in
/// memory. Periodically, the data is uploaded to the cloud storage
/// and removed from memory.
template<class Clock = ss::lowres_clock>
class batcher {
    using clock_t = Clock;
    using timestamp_t = typename Clock::time_point;

    friend struct batcher_accessor;

public:
    explicit batcher(
      write_pipeline<Clock>::stage stage,
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote_api<Clock>& remote_api,
      cloud_topics::cluster_services* cluster_services);

    ss::future<> start();
    ss::future<> stop();

private:
    /// Run one iteration of the background loop
    ///
    /// Single call
    /// - filters out timed out requests
    /// - aggregates requests to create one L0 object
    /// - uploads L0 object
    /// - generates placeholders and propagates them
    ///
    /// \returns error code
    ss::future<std::expected<std::monostate, errc>>
      run_once(write_pipeline<Clock>::write_requests_list) noexcept;

    /// Background fiber responsible for merging
    /// aggregated log data and sending it to the
    /// cloud storage
    ///
    /// The method should only be invoked on shard 0
    ss::future<> bg_controller_loop();

    /// Upload L0 object based on placeholders
    ///
    /// Collect data from every shard and upload stream of data to S3.
    ///
    /// \return size of the uploaded object or error code
    ss::future<std::expected<size_t, errc>>
    upload_object(object_id id, iobuf payload);

    cloud_topics::cluster_services* _cluster_services;
    cloud_io::remote_api<Clock>& _remote;
    cloud_storage_clients::bucket_name _bucket;
    config::binding<std::chrono::milliseconds> _upload_timeout;
    config::binding<std::chrono::milliseconds> _upload_backoff_interval;
    config::binding<std::chrono::milliseconds> _upload_interval;

    ss::gate _gate;
    ss::abort_source _as;

    static constexpr size_t max_buffer_size = 16_MiB;
    static constexpr size_t max_cardinality = 1000;

    basic_retry_chain_node<Clock> _rtc;
    basic_retry_chain_logger<Clock> _logger;

    write_pipeline<Clock>::stage _stage;

    batcher_probe _probe;
};
} // namespace cloud_topics::l0
