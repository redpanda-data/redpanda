// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/snc_quota_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "prometheus/prometheus_sanitize.h"

using namespace std::chrono_literals;

namespace kafka {

snc_quota_manager::snc_quota_manager()
  : _max_kafka_throttle_delay(
    config::shard_local_cfg().max_kafka_throttle_delay_ms.bind())
  , _kafka_throughput_limit_node_in_bps(
      config::shard_local_cfg().kafka_throughput_limit_node_in_bps.bind())
  , _kafka_throughput_limit_node_out_bps(
      config::shard_local_cfg().kafka_throughput_limit_node_out_bps.bind())
  , _kafka_quota_balancer_window(
      config::shard_local_cfg().kafka_quota_balancer_window.bind())
  , _kafka_quota_balancer_node_period(
      config::shard_local_cfg().kafka_quota_balancer_node_period.bind())
  , _kafka_quota_balancer_min_shard_thoughput_ratio(
      config::shard_local_cfg()
        .kafka_quota_balancer_min_shard_thoughput_ratio.bind())
  , _kafka_quota_balancer_min_shard_thoughput_bps(
      config::shard_local_cfg()
        .kafka_quota_balancer_min_shard_thoughput_bps.bind())
  , _shard_ingress_quota(
      get_shard_ingress_quota_default(), _kafka_quota_balancer_window())
  , _shard_egress_quota(
      get_shard_egress_quota_default(), _kafka_quota_balancer_window()) {
    _kafka_throughput_limit_node_in_bps.watch([this] {
        _shard_ingress_quota.set_quota(get_shard_ingress_quota_default());
    });
    _kafka_throughput_limit_node_out_bps.watch([this] {
        _shard_egress_quota.set_quota(get_shard_egress_quota_default());
    });
    _kafka_quota_balancer_window.watch([this] {
        const auto v = _kafka_quota_balancer_window();
        vlog(klog.debug, "Set shard TP token bucket window {}", v);
        _shard_ingress_quota.set_width(v);
        _shard_egress_quota.set_width(v);
    });
}

ss::future<> snc_quota_manager::start() { return ss::make_ready_future<>(); }

ss::future<> snc_quota_manager::stop() { return ss::make_ready_future<>(); }

namespace {

bottomless_token_bucket::quota_t get_default_quota_from_property(
  const config::binding<std::optional<int64_t>>& prop) {
    if (prop()) {
        const bottomless_token_bucket::quota_t v = *prop() / ss::smp::count;
        if (v > bottomless_token_bucket::max_quota) {
            return bottomless_token_bucket::max_quota;
        }
        if (v < 1) {
            return 1;
        }
        return v;
    } else {
        return bottomless_token_bucket::max_quota;
    }
}

using delay_t = std::chrono::milliseconds;

/// Evaluate throttling delay required based on the state of a token bucket
delay_t eval_delay(const bottomless_token_bucket& tb) noexcept {
    // no delay if tokens are over the bottom
    if (tb.tokens() >= 0) {
        return delay_t::zero();
    }
    // quota is in [tokens/s], so we need to scale it to `delay_t` by dividing
    // by the `delay_t` unit scale denominator (and multiplying by the numerator
    // but that one has to be 1)
    static_assert(delay_t::period::num == 1);
    return delay_t(muldiv(-tb.tokens(), delay_t::period::den, tb.quota()));
}

} // namespace

snc_quota_manager::quota_t
snc_quota_manager::get_shard_ingress_quota_default() const {
    const quota_t v = get_default_quota_from_property(
      _kafka_throughput_limit_node_in_bps);
    vlog(klog.debug, "Default shard TP ingress quota: {}", v);
    return v;
}

snc_quota_manager::quota_t
snc_quota_manager::get_shard_egress_quota_default() const {
    const quota_t v = get_default_quota_from_property(
      _kafka_throughput_limit_node_out_bps);
    vlog(klog.debug, "Default shard TP egress quota {}", v);
    return v;
}

snc_quota_manager::delays_t snc_quota_manager::get_shard_delays(
  clock::time_point& connection_throttle_until,
  const clock::time_point now) const {
    delays_t res;

    // force throttle whatever the client did not do on its side
    if (now < connection_throttle_until) {
        res.enforce = connection_throttle_until - now;
    }

    // throttling delay the connection should be requested to throttle
    // this time
    res.request = std::min(
      _max_kafka_throttle_delay(),
      std::max(
        eval_delay(_shard_ingress_quota), eval_delay(_shard_egress_quota)));
    connection_throttle_until = now + res.request;

    return res;
}

void snc_quota_manager::record_request_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_ingress_quota.use(request_size, now);
}

void snc_quota_manager::record_response_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_egress_quota.use(request_size, now);
}

} // namespace kafka
