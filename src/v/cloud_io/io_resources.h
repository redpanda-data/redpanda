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
#include "config/property.h"
#include "ssx/semaphore.h"
#include "utils/adjustable_semaphore.h"
#include "utils/token_bucket.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace cloud_io {

/**
 * This class tracks ongoing object downloads.
 */
class io_resources {
    friend class throttled_dl_source;

public:
    io_resources();

    ss::future<> start();
    ss::future<> stop();

    ss::future<ssx::semaphore_units> get_hydration_units(size_t n);

    /// Throttles the given stream such that calls to get on the resulting
    /// stream will wait some time if the underlying `_throughput_limit` is
    /// under stress.
    ss::input_stream<char> throttle_download(
      ss::input_stream<char> underlying,
      ss::abort_source& as,
      std::function<void(size_t)> throttle_metric_ms_cb = {});

    /// How many partition_record_batch_reader_impl instances exist
    size_t current_ongoing_hydrations() const;

private:
    config::binding<std::optional<uint32_t>>
      _max_concurrent_hydrations_per_shard;

    size_t max_parallel_hydrations() const;

    /// Set bandwidth for tiered-storage scheduling_group
    ss::future<> set_disk_max_bandwidth(size_t tput);

    /// Set download bandwidth for cloud storage API
    void set_net_max_bandwidth(size_t tput);

    /// Recalculate and reset throughput limits
    ss::future<> update_throughput();

    /// Gate for background eviction
    ss::gate _gate;

    adjustable_semaphore _hydration_units;

    token_bucket<> _throughput_limit;
    config::binding<std::optional<size_t>> _throughput_shard_limit_config;
    config::binding<std::optional<size_t>> _relative_throughput;
    bool _throttling_disabled{false};
    std::optional<size_t> _device_throughput;
};

} // namespace cloud_io
