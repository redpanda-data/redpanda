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

#include <fmt/core.h>

using namespace std::chrono_literals;

namespace kafka {

using quota_t = snc_quota_manager::quota_t;

template<std::integral T>
ingress_egress_state<T> operator+(
  const ingress_egress_state<T>& lhs, const ingress_egress_state<T>& rhs) {
    return {.in = lhs.in + rhs.in, .eg = lhs.eg + rhs.eg};
}
template<std::integral T>
ingress_egress_state<T> operator-(
  const ingress_egress_state<T>& lhs, const ingress_egress_state<T>& rhs) {
    return {.in = lhs.in - rhs.in, .eg = lhs.eg - rhs.eg};
}
template<std::integral T>
ingress_egress_state<T> operator-(const ingress_egress_state<T>& v) {
    return {.in = -v.in, .eg = -v.eg};
}
template<std::integral T>
bool is_zero(const ingress_egress_state<T>& v) noexcept {
    return v.in == 0 && v.eg == 0;
}

/// Copies vector of inoutpairs to an ingress_egress_state of vectors
template<class T>
ingress_egress_state<std::vector<T>>
to_soa_layout(const std::vector<ingress_egress_state<T>>& v) {
    ingress_egress_state<std::vector<T>> res;
    res.in.reserve(v.size());
    res.eg.reserve(v.size());
    for (const ingress_egress_state<T>& i : v) {
        res.in.push_back(i.in);
        res.eg.push_back(i.eg);
    };
    return res;
}

namespace {

quota_t
node_to_shard_quota(const config::binding<std::optional<quota_t>> node_quota) {
    if (node_quota().has_value()) {
        const quota_t v = node_quota().value() / ss::smp::count;
        if (v > bottomless_token_bucket::max_quota) {
            return bottomless_token_bucket::max_quota;
        }
        if (v < 1) {
            return quota_t{1};
        }
        return v;
    } else {
        return bottomless_token_bucket::max_quota;
    }
}

} // namespace

snc_quota_manager::snc_quota_manager()
  : _max_kafka_throttle_delay(
    config::shard_local_cfg().max_kafka_throttle_delay_ms.bind())
  , _kafka_throughput_limit_node_bps{
      config::shard_local_cfg().kafka_throughput_limit_node_in_bps.bind(),
      config::shard_local_cfg().kafka_throughput_limit_node_out_bps.bind()}
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
  , _shard_quota{
        .in {node_to_shard_quota(_kafka_throughput_limit_node_bps.in),
             _kafka_quota_balancer_window()},
        .eg {node_to_shard_quota(_kafka_throughput_limit_node_bps.eg),
             _kafka_quota_balancer_window()}}
{
    _kafka_quota_balancer_window.watch([this] {
        const auto v = _kafka_quota_balancer_window();
        vlog(klog.debug, "qm - Set shard TP token bucket window: {}", v);
        _shard_quota.in.set_width(v);
        _shard_quota.eg.set_width(v);
    });

    vlog(klog.debug, "qm - Initial quota: {}", _shard_quota);
}

ss::future<> snc_quota_manager::start() { return ss::make_ready_future<>(); }

ss::future<> snc_quota_manager::stop() { return ss::make_ready_future<>(); }

namespace {

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
      std::max(eval_delay(_shard_quota.in), eval_delay(_shard_quota.eg)));
    connection_throttle_until = now + res.request;

    return res;
}

void snc_quota_manager::record_request_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_quota.in.use(request_size, now);
}

void snc_quota_manager::record_response_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_quota.eg.use(request_size, now);
}

} // namespace kafka

struct parseless_formatter {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        return ctx.begin();
    }
};

template<class T>
struct fmt::formatter<kafka::ingress_egress_state<T>> : parseless_formatter {
    template<typename Ctx>
    auto format(const kafka::ingress_egress_state<T>& p, Ctx& ctx) const {
        return fmt::format_to(ctx.out(), "(in:{}, eg:{})", p.in, p.eg);
    }
};
