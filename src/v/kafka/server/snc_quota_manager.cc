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
#include <fmt/ranges.h>

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

using shard_count_t = unsigned;

struct borrower_t {
    ss::shard_id shard_id;
    // how many borrowers the shard counts towards as, can be 0 or 1
    ingress_egress_state<shard_count_t> borrowers_count;
};

ss::future<> snc_quota_manager::quota_balancer_step() {
    const auto units = co_await _balancer_mx.get_units();
    _balancer_gate.check();
    _balancer_timer_last_ran = ss::lowres_clock::now();
    vlog(klog.trace, "qb - Step");

    // determine the borrowers and whether any balancing is needed now
    const auto borrowers = co_await container().map(
      [](snc_quota_manager& qm) -> borrower_t {
          qm.refill_buckets(clock::now());
          const ingress_egress_state<quota_t> def = qm.get_deficiency();
          return {
            .shard_id = ss::this_shard_id(),
            .borrowers_count{
              .in = def.in > 0 ? 1u : 0u, .eg = def.eg > 0 ? 1u : 0u}};
      });

    const auto borrowers_count = std::accumulate(
      borrowers.cbegin(),
      borrowers.cend(),
      ingress_egress_state<shard_count_t>{0u, 0u},
      [](const ingress_egress_state<shard_count_t>& s, const borrower_t& b) {
          return s + b.borrowers_count;
      });
    vlog(
      klog.trace,
      "qb - Borrowers count: {}, borrowers:",
      borrowers_count,
      borrowers);
    if (is_zero(borrowers_count)) {
        co_return;
    }

    // collect quota from lenders
    const ingress_egress_state<quota_t> collected
      = co_await container().map_reduce0(
        [borrowers_count](snc_quota_manager& qm) {
            const auto surplus_to_collect =
              [](const quota_t surplus, const shard_count_t borrowers_count) {
                  return borrowers_count > 0 ? surplus / 2 : 0;
              };
            const ingress_egress_state<quota_t> surplus = qm.get_surplus();
            const ingress_egress_state<quota_t> to_collect{
              .in = surplus_to_collect(surplus.in, borrowers_count.in),
              .eg = surplus_to_collect(surplus.eg, borrowers_count.eg)};
            qm.adjust_quota(-to_collect);
            return to_collect;
        },
        ingress_egress_state<quota_t>{0, 0},
        std::plus{});
    vlog(klog.trace, "qb - Collected: {}", collected);

    // dispense the collected amount among the borrowers
    using split_t = decltype(std::div(quota_t{}, shard_count_t{}));
    ingress_egress_state<split_t> split{{0, 0}, {0, 0}};
    if (borrowers_count.in > 0) {
        split.in = std::ldiv(collected.in, borrowers_count.in);
    }
    if (borrowers_count.eg > 0) {
        split.eg = std::ldiv(collected.eg, borrowers_count.eg);
    }

    const auto equal_share_or_0 =
      [](split_t& split, const shard_count_t borrowers_count) -> quota_t {
        if (borrowers_count == 0) {
            return 0;
        }
        if (split.rem == 0) {
            return split.quot;
        }
        --split.rem;
        return split.quot + 1;
    };
    for (const borrower_t& b : borrowers) {
        const ingress_egress_state<quota_t> share{
          .in = equal_share_or_0(split.in, b.borrowers_count.in),
          .eg = equal_share_or_0(split.eg, b.borrowers_count.eg)};
        if (!is_zero(share)) {
            // TBD: make the invocation parallel
            co_await container().invoke_on(
              b.shard_id,
              [share](snc_quota_manager& qm) { qm.adjust_quota(share); });
        }
    }
    if (split.in.rem != 0 || split.eg.rem != 0) {
        vlog(klog.error, "qb - Split not dispensed in full: {}", split);
    }
}

namespace {

/// Split \p value between the elements of vector \p target in full, adding them
/// to the elements already in the vector.
/// If \p value is not a multiple of target.size(), the entire \p value
/// is still dispensed in full. Quotinents rounded towards infinity
/// are added to the front elements of the \p target, and those rounded
/// towards zero are added to the back elements.
void dispense_equally(std::vector<quota_t>& target, const quota_t value) {
    auto share = std::div(value, target.size());
    for (quota_t& v : target) {
        v += share.quot;
        if (share.rem > 0) {
            v += 1;
            share.quot -= 1;
        } else if (share.rem < 0) {
            v -= 1;
            share.quot += 1;
        }
    }
}

/// If \p value is less than \p limit, set it to the \p limit value and
/// return the difference as positive number. Otherwise \p value is unchanged
/// and 0 is returned
quota_t cap_to_ceiling(quota_t& value, const quota_t limit) {
    if (const quota_t d = limit - value; d > 0) {
        value += d;
        return d;
    }
    return 0;
}

/// If \p delta is negative, splits its value accourding to \p surplus values:
/// - amount under total surplus is split pro rata to \p surplus amounts
/// - amount over total surplus is split equally. Values in \p schedule
/// are added to.
/// \pre schedule.size() == surplus.size()
void dispense_negative_deltas(
  std::vector<quota_t>& schedule,
  quota_t& delta,
  const std::vector<quota_t>& surplus) {
    if (delta >= 0) {
        return;
    }

    const quota_t total_surplus = std::reduce(
      surplus.cbegin(), surplus.cend(), quota_t{0}, std::plus{});

    if (delta > -total_surplus) {
        quota_t remainder = delta;
        // pro rata to surpluses
        for (size_t k = 0; k != ss::smp::count; ++k) {
            const quota_t share = muldiv(delta, surplus.at(k), total_surplus);
            schedule.at(k) += share;
            remainder -= share;
        }
        // the remainder equally
        if (remainder > 0) {
            vlog(
              klog.error,
              "qb - Expected {} <= 0; delta: {}, total_surplus: {}, surpluses: "
              "{}",
              remainder,
              delta,
              total_surplus,
              surplus);
            return;
        }
        if (remainder < 0) {
            const quota_t d = (remainder + 1)
                                / static_cast<quota_t>(schedule.size())
                              - 1;
            for (quota_t& s : schedule) {
                const quota_t dd = std::max(d, remainder);
                remainder -= dd;
                s += dd;
            }
        }

    } else { // delta <= -total_surplus

        // all surpluses
        for (size_t k = 0; k != ss::smp::count; ++k) {
            const quota_t share = -surplus.at(k);
            schedule.at(k) += share;
            delta -= share;
        }
        // the rest equally
        auto share = std::div(delta, ss::smp::count);
        for (quota_t& s : schedule) {
            s += share.quot;
            if (share.rem < 0) {
                s += -1;
                share.quot -= -1;
            }
        }
    }
}

} // namespace

ss::future<> snc_quota_manager::quota_balancer_update(
  ingress_egress_state<dispense_quota_amount> shard_quotas_update) {
    const auto units = co_await _balancer_mx.get_units();
    _balancer_gate.check();
    vlog(klog.trace, "qb - Update: {}", shard_quotas_update);

    // deliver shard quota updates of value type
    if (
      shard_quotas_update.in.amount_is == dispense_quota_amount::amount_t::value
      || shard_quotas_update.eg.amount_is
           == dispense_quota_amount::amount_t::value) {
        co_await container().invoke_on_all(
          [shard_quotas_update](snc_quota_manager& qm) {
              qm.maybe_set_quota(
                {.in = shard_quotas_update.in.get_value(),
                 .eg = shard_quotas_update.eg.get_value()});
          });
    }

    // deliver deltas and try to dispense them fairly and in full
    ingress_egress_state<quota_t> deltas = {
      .in = shard_quotas_update.in.get_delta_or_0(),
      .eg = shard_quotas_update.eg.get_delta_or_0()};
    if (is_zero(deltas)) {
        co_return;
    }

    ingress_egress_state<std::vector<quota_t>> schedule{
      std::vector<quota_t>(ss::smp::count),
      std::vector<quota_t>(ss::smp::count)};

    if (deltas.in < 0 || deltas.eg < 0) {
        // cap negative delta at -(total surrenderable quota),
        // diff towards node deficit
        const auto total_quota = co_await container().map_reduce0(
          [](const snc_quota_manager& qm) {
              return qm.get_quota() - qm._shard_quota_minimum;
          },
          ingress_egress_state<quota_t>{0, 0},
          std::plus{});
        _node_deficit.in += cap_to_ceiling(deltas.in, -total_quota.in);
        _node_deficit.eg += cap_to_ceiling(deltas.eg, -total_quota.eg);

        const auto surplus_aos = co_await container().map(
          [](snc_quota_manager& qm) {
              qm.refill_buckets(clock::now());
              return qm.get_surplus();
          });

        const auto surplus_soa = to_soa_layout(surplus_aos);
        dispense_negative_deltas(schedule.in, deltas.in, surplus_soa.in);
        dispense_negative_deltas(schedule.eg, deltas.eg, surplus_soa.eg);
    }
    if (deltas.in > 0) {
        dispense_equally(schedule.in, deltas.in);
    }
    if (deltas.eg > 0) {
        dispense_equally(schedule.eg, deltas.eg);
    }

    vlog(
      klog.debug,
      "qb - Dispense delta updates {} of {} as {}",
      deltas,
      shard_quotas_update,
      schedule);

    co_await container().invoke_on_all([&schedule](snc_quota_manager& qm) {
        qm.adjust_quota(
          {.in = schedule.in.at(ss::this_shard_id()),
           .eg = schedule.eg.at(ss::this_shard_id())});
        vlog(klog.trace, "qb - Delta updates applied: {}", qm._shard_quota);
    });
}

void snc_quota_manager::refill_buckets(const clock::time_point now) noexcept {
    _shard_quota.in.refill(now);
    _shard_quota.eg.refill(now);
}

ingress_egress_state<quota_t>
snc_quota_manager::get_deficiency() const noexcept {
    const auto f = [](
                     const bottomless_token_bucket& b,
                     const quota_t shard_quota_min) -> quota_t {
        if (b.tokens() < 0 || b.quota() < shard_quota_min) {
            return 1;
        }
        return 0;
    };
    return {
      .in = f(_shard_quota.in, _shard_quota_minimum.in),
      .eg = f(_shard_quota.eg, _shard_quota_minimum.eg)};
}

ingress_egress_state<quota_t> snc_quota_manager::get_quota() const noexcept {
    return {.in = _shard_quota.in.quota(), .eg = _shard_quota.eg.quota()};
}

ingress_egress_state<quota_t> snc_quota_manager::get_surplus() const noexcept {
    return {
      .in = std::max(
        _shard_quota.in.get_current_rate() - _shard_quota_minimum.in,
        quota_t{0}),
      .eg = std::max(
        _shard_quota.eg.get_current_rate() - _shard_quota_minimum.eg,
        quota_t{0})};
}

void snc_quota_manager::maybe_set_quota(
  const ingress_egress_state<std::optional<quota_t>>& value) noexcept {
    if (value.in.has_value()) {
        _shard_quota.in.set_quota(value.in.value());
    }
    if (value.eg.has_value()) {
        _shard_quota.eg.set_quota(value.eg.value());
    }
    vlog(klog.trace, "qm - Set quota: {} -> {}", value, _shard_quota);
}

void snc_quota_manager::adjust_quota(
  const ingress_egress_state<quota_t>& delta) noexcept {
    _shard_quota.in.set_quota(_shard_quota.in.quota() + delta.in);
    _shard_quota.eg.set_quota(_shard_quota.eg.quota() + delta.eg);
    vlog(klog.trace, "qm - Adjust quota: {} -> {}", delta, _shard_quota);
}

snc_quota_manager::quota_t
snc_quota_manager::dispense_quota_amount::get_delta_or_0() const noexcept {
    if (amount_is == amount_t::delta) {
        return amount;
    } else {
        return 0;
    }
}

std::optional<snc_quota_manager::quota_t>
snc_quota_manager::dispense_quota_amount::get_value() const noexcept {
    if (amount_is == amount_t::value) {
        return amount;
    } else {
        return std::nullopt;
    }
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
struct fmt::formatter<kafka::borrower_t> : parseless_formatter {
    template<typename Ctx>
    auto format(const kafka::borrower_t& v, Ctx& ctx) const {
        return fmt::format_to(
          ctx.out(), "{{{}, {}}}", v.shard_id, v.borrowers_count);
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

template<>
struct fmt::formatter<std::ldiv_t> : parseless_formatter {
    template<typename Ctx>
    auto format(const std::ldiv_t& v, Ctx& ctx) const {
        return fmt::format_to(ctx.out(), "(q:{}, r:{})", v.quot, v.rem);
    }
};
