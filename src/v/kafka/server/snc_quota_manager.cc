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

static constexpr ss::shard_id quota_balancer_shard = 0;

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

quota_t node_to_shard_quota(const std::optional<quota_t> node_quota) {
    if (node_quota.has_value()) {
        const quota_t v = node_quota.value() / ss::smp::count;
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
  , _node_quota_default{calc_node_quota_default()}
  , _shard_quota{
      .in {node_to_shard_quota(_node_quota_default.in),
           _kafka_quota_balancer_window()},
      .eg {node_to_shard_quota(_node_quota_default.eg),
           _kafka_quota_balancer_window()}}
{
    update_shard_quota_minimum();
    _kafka_throughput_limit_node_bps.in.watch(
      [this] { update_node_quota_default(); });
    _kafka_throughput_limit_node_bps.eg.watch(
      [this] { update_node_quota_default(); });
    _kafka_quota_balancer_window.watch([this] {
        const auto v = _kafka_quota_balancer_window();
        vlog(klog.debug, "qm - Set shard TP token bucket window: {}", v);
        _shard_quota.in.set_width(v);
        _shard_quota.eg.set_width(v);
    });
    _kafka_quota_balancer_node_period.watch([this] {
        if (_balancer_gate.is_closed()) {
            return;
        }
        if (_balancer_timer.cancel()) {
            // cancel() returns true only on the balancer shard
            // because the timer is never armed on the others
            arm_balancer_timer();
        }
    });
    _kafka_quota_balancer_min_shard_thoughput_ratio.watch(
      [this] { update_shard_quota_minimum(); });
    _kafka_quota_balancer_min_shard_thoughput_bps.watch(
      [this] { update_shard_quota_minimum(); });

    if (ss::this_shard_id() == quota_balancer_shard) {
        _balancer_timer.set_callback([this] {
            ssx::spawn_with_gate(_balancer_gate, [this] {
                return quota_balancer_step().finally(
                  [this] { arm_balancer_timer(); });
            });
        });
    }
    vlog(klog.debug, "qm - Initial quota: {}", _shard_quota);
}

ss::future<> snc_quota_manager::start() {
    if (ss::this_shard_id() == quota_balancer_shard) {
        _balancer_timer.arm(
          ss::lowres_clock::now() + get_quota_balancer_node_period());
    }
    return ss::make_ready_future<>();
}

ss::future<> snc_quota_manager::stop() {
    if (ss::this_shard_id() == quota_balancer_shard) {
        _balancer_timer.cancel();
        return _balancer_gate.close();
    } else {
        return ss::make_ready_future<>();
    }
}

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

ingress_egress_state<std::optional<quota_t>>
snc_quota_manager::calc_node_quota_default() const {
    // here will be the code to merge node limit
    // and node share of cluster limit; so far it's node limit only
    const ingress_egress_state<std::optional<quota_t>> default_quota{
      .in = _kafka_throughput_limit_node_bps.in(),
      .eg = _kafka_throughput_limit_node_bps.eg()};
    vlog(klog.trace, "qm - Default node TP quotas: {}", default_quota);
    return default_quota;
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

ss::lowres_clock::duration
snc_quota_manager::get_quota_balancer_node_period() const {
    const auto v = _kafka_quota_balancer_node_period();
    // zero period in config means do not run balancer
    if (v == std::chrono::milliseconds::zero()) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          ss::lowres_clock::duration::max() / 2);
    }
    return v;
}

void snc_quota_manager::update_node_quota_default() {
    const ingress_egress_state<std::optional<quota_t>> new_node_quota_default
      = calc_node_quota_default();
    if (ss::this_shard_id() == quota_balancer_shard) {
        // downstream updates:
        // - shard effective quota (via _shard_quotas_update):
        const auto calc_diff = [](
                                 const std::optional<quota_t>& qold,
                                 const std::optional<quota_t>& qnew) {
            dispense_quota_amount diff;
            if (qold && qnew) {
                diff.amount = *qnew - *qold;
                diff.amount_is = dispense_quota_amount::amount_t::delta;
            } else if (qold || qnew) {
                diff.amount = node_to_shard_quota(qnew);
                diff.amount_is = dispense_quota_amount::amount_t::value;
            }
            return diff;
        };
        ingress_egress_state<dispense_quota_amount> shard_quotas_update{
          .in = calc_diff(_node_quota_default.in, new_node_quota_default.in),
          .eg = calc_diff(_node_quota_default.eg, new_node_quota_default.eg)};
        ssx::spawn_with_gate(
          _balancer_gate, [this, update = shard_quotas_update] {
              return quota_balancer_update(update);
          });
    }
    _node_quota_default = new_node_quota_default;
    // - shard minimum quota:
    update_shard_quota_minimum();
}

void snc_quota_manager::update_shard_quota_minimum() {
    const auto f = [this](const std::optional<quota_t>& node_quota_default) {
        const auto shard_quota_default = node_to_shard_quota(
          node_quota_default);
        return std::max<quota_t>(
          _kafka_quota_balancer_min_shard_thoughput_ratio()
            * shard_quota_default,
          _kafka_quota_balancer_min_shard_thoughput_bps());
    };
    _shard_quota_minimum.in = f(_node_quota_default.in);
    _shard_quota_minimum.eg = f(_node_quota_default.eg);
    // downstream updates: none
}

void snc_quota_manager::arm_balancer_timer() {
    static constexpr auto min_sleep_time = 2ms; // TBD config?
    ss::lowres_clock::time_point arm_until = _balancer_timer_last_ran
                                             + get_quota_balancer_node_period();
    const auto now = ss::lowres_clock::now();
    const ss::lowres_clock::time_point closest_arm_until = now + min_sleep_time;
    if (arm_until < closest_arm_until) {
        // occurs when balancer takes to run longer than the balancer period,
        // and on the first iteration
        if (
          _balancer_timer_last_ran.time_since_epoch()
          != ss::lowres_clock::duration::zero()) {
            vlog(
              klog.warn,
              "qb - Quota balancer is invoked too often ({}), "
              "enforcing minimum sleep time",
              arm_until - now);
        }
        arm_until = closest_arm_until;
    }
    _balancer_timer.arm(arm_until);
}

ss::future<> snc_quota_manager::quota_balancer_step() {
    _balancer_gate.check();
    _balancer_timer_last_ran = ss::lowres_clock::now();
    return ss::make_ready_future<>();
}

ss::future<> snc_quota_manager::quota_balancer_update(
  ingress_egress_state<dispense_quota_amount> /*shard_quotas_update*/) {
    _balancer_gate.check();
    return ss::make_ready_future<>();
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

template<>
struct fmt::formatter<kafka::snc_quota_manager::dispense_quota_amount>
  : parseless_formatter {
    template<typename Ctx>
    auto format(
      const kafka::snc_quota_manager::dispense_quota_amount& v,
      Ctx& ctx) const {
        const char* ai_name;
        switch (v.amount_is) {
        case kafka::snc_quota_manager::dispense_quota_amount::amount_t::delta:
            ai_name = "delta";
            break;
        case kafka::snc_quota_manager::dispense_quota_amount::amount_t::value:
            ai_name = "value";
            break;
        default:
            ai_name = "?";
            break;
        }
        return fmt::format_to(ctx.out(), "{{{}: {}}}", ai_name, v.amount);
    }
};
