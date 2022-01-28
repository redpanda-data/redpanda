/*
 * Copyright 2020 Vectorized, Inc.
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
#include "resource_mgmt/rate.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>
#include <string_view>

namespace kafka {

// quota_manager tracks quota usage
//
// TODO:
//   - we will want to eventually add support for configuring the quotas and
//   quota settings as runtime through the kafka api and other mechanisms.
//
//   - currently only total throughput per client_id is tracked. in the future
//   we will want to support additional quotas and accouting granularities to be
//   at parity with kafka. for example:
//
//      - splitting out rates separately for produce and fetch
//      - accounting per user vs per client (these are separate in kafka)
//
//   - it may eventually be beneficial to periodically reduce stats across
//   shards or track stats globally to produce a more accurate per-node
//   representation of a statistic (e.g. bandwidth).
//
class quota_manager {
public:
    using clock = ss::lowres_clock;

    struct throttle_delay {
        bool first_violation;
        clock::duration duration;
    };

    quota_manager()
      : _default_num_windows(
        config::shard_local_cfg().default_num_windows.bind())
      , _default_window_width(
          config::shard_local_cfg().default_window_sec.bind())
      , _target_tp_rate(config::shard_local_cfg().target_quota_byte_rate.bind())
      , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
      , _max_delay(
          config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
        _gc_timer.set_callback([this] {
            auto full_window = _default_num_windows() * _default_window_width();
            gc(full_window);
        });
    }

    quota_manager(const quota_manager&) = delete;
    quota_manager& operator=(const quota_manager&) = delete;
    quota_manager(quota_manager&&) = delete;
    quota_manager& operator=(quota_manager&&) = delete;

    ~quota_manager();

    ss::future<> stop();

    ss::future<> start();

    // record a new observation and return <previous delay, new delay>
    throttle_delay record_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

private:
    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc(clock::duration full_window);

private:
    // last_seen: used for gc keepalive
    // delay: last calculated delay
    // tp_rate: throughput tracking
    struct quota {
        clock::time_point last_seen;
        clock::duration delay;
        rate_tracker tp_rate;
    };

    config::binding<int16_t> _default_num_windows;
    config::binding<clock::duration> _default_window_width;

    config::binding<uint32_t> _target_tp_rate;
    absl::flat_hash_map<ss::sstring, quota> _quotas;

    ss::timer<> _gc_timer;
    clock::duration _gc_freq;
    config::binding<clock::duration> _max_delay;
};

} // namespace kafka
