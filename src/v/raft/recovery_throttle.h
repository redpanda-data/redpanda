/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "config/configuration.h"
#include "config/property.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/logger.h"
#include "seastarx.h"
#include "ssx/metrics.h"
#include "ssx/semaphore.h"
#include "utils/token_bucket.h"

#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/later.hh>

namespace raft {

/*
 * Token bucket-based raft recovery throttling.
 *
 * Improvements
 *
 *  - cross-core bandwidth sharing
 *      https://github.com/redpanda-data/redpanda/issues/1770
 *
 *  - cluster-level recovery control
 *      https://github.com/redpanda-data/redpanda/issues/1771
 */
class recovery_throttle {
public:
    explicit recovery_throttle(config::binding<size_t> rate_binding)
      : _rate_binding(std::move(rate_binding))
      , _throttler(get_per_core_rate(), "raft/recovery-rate") {
        _rate_binding.watch([this]() { update_rate(); });
        setup_metrics();
    }

    ss::future<> throttle(size_t size, ss::abort_source& as) {
        return _throttler.throttle(size, as);
    }

    size_t available() { return _throttler.available(); }

    void shutdown() { _throttler.shutdown(); }

private:
    size_t get_per_core_rate() { return _rate_binding() / ss::smp::count; }

    void update_rate() {
        auto const per_core_rate = get_per_core_rate();
        _throttler.update_rate(per_core_rate);
        vlog(
          raftlog.info,
          "Updating recovery throttle with new rate of {}",
          per_core_rate);
    }

    void setup_metrics() {
        setup_internal_metrics();
        setup_public_metrics();
    }

    void setup_internal_metrics() {
        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        // TODO: Delete this metric after public is supported in scraping
        // configurations
        namespace sm = ss::metrics;
        _internal_metrics.add_group(
          prometheus_sanitize::metrics_name("raft:recovery"),
          {sm::make_gauge(
            "partition_movement_available_bandwidth",
            [this] { return _throttler.available(); },
            sm::description(
              "Bandwidth available for partition movement. bytes/sec"))});
    }

    void setup_public_metrics() {
        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        namespace sm = ss::metrics;
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("raft:recovery"),
          {sm::make_gauge(
            "partition_movement_available_bandwidth",
            [this] { return _throttler.available(); },
            sm::description(
              "Bandwidth available for partition movement. bytes/sec"))});
    }

    config::binding<size_t> _rate_binding;
    token_bucket<> _throttler;
    ss::metrics::metric_groups _internal_metrics;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

} // namespace raft
