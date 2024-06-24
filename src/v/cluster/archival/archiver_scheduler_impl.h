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

#include "cluster/archival/archiver_scheduler_api.h"
#include "config/property.h"
#include "utils/token_bucket.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <absl/container/btree_map.h>

#include <chrono>

namespace archival {

namespace detail {

struct ntp_scheduler_state {
    /// Next sleep interval
    std::optional<std::chrono::milliseconds> backoff;
    ss::gate gate;
};

}; // namespace detail

template<class Clock = ss::lowres_clock>
class archiver_scheduler : public archiver_scheduler_api<Clock> {
public:
    explicit archiver_scheduler(
      size_t upload_tput_rate, size_t upload_requests_rate);

    ss::future<result<upload_resource_quota>>
    maybe_suspend_upload(upload_resource_usage<Clock> arg) noexcept override;

    ss::future<> create_ntp_state(model::ntp) override;
    ss::future<> dispose_ntp_state(model::ntp) override;

    ss::future<> start() override;

    ss::future<> stop() override;

private:
    // Global shard throughput limit
    token_bucket<Clock> _shard_tput_limit;
    /// Global limit on PUT request rate
    token_bucket<Clock> _put_requests;

    // Per-ntp state
    absl::btree_map<model::ntp, std::unique_ptr<detail::ntp_scheduler_state>>
      _partitions;

    config::binding<std::chrono::milliseconds> _initial_backoff;
    config::binding<std::chrono::milliseconds> _max_backoff;

    ss::gate _gate;
};

} // namespace archival
