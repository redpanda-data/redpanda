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
#include "tristate.h"

#include <seastar/core/metrics.hh>

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

void snc_quotas_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    const auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                                    ? std::vector<sm::label>{sm::shard_label}
                                    : std::vector<sm::label>{};
    std::vector<ss::metrics::metric_definition> metric_defs;
    static const auto direction_label = ss::metrics::label("direction");
    static const auto label_ingress = direction_label("ingress");
    static const auto label_egress = direction_label("egress");

    if (ss::this_shard_id() == quota_balancer_shard) {
        metric_defs.emplace_back(sm::make_counter(
          "balancer_runs",
          _balancer_runs,
          sm::description(
            "Number of times throughput quota balancer has been run")));
    }
    {
        static const char* name = "quota_effective";
        static const auto desc = sm::description(
          "Currently effective quota, in bytes/s");
        metric_defs.emplace_back(
          sm::make_counter(
            name, [this] { return _qm.get_quota().in; }, desc, {label_ingress})
            .aggregate(aggregate_labels));
        metric_defs.emplace_back(
          sm::make_counter(
            name, [this] { return _qm.get_quota().eg; }, desc, {label_egress})
            .aggregate(aggregate_labels));
    }
    metric_defs.emplace_back(
      sm::make_counter(
        "traffic_intake",
        _traffic_in,
        sm::description("Amount of Kafka traffic received from the clients "
                        "that is taken into processing, in bytes"))
        .aggregate(aggregate_labels));

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:quotas"), metric_defs);
}

namespace {

quota_t node_to_shard_quota(const std::optional<quota_t> node_quota) {
    if (node_quota.has_value()) {
        const auto v = std::div(node_quota.value(), ss::smp::count);
        if (v.quot >= bottomless_token_bucket::max_quota) {
            return bottomless_token_bucket::max_quota;
        }
        if (v.quot < bottomless_token_bucket::min_quota) {
            return bottomless_token_bucket::min_quota;
        }
        if (v.rem > ss::this_shard_id()) {
            return v.quot + 1;
        } else {
            return v.quot;
        }
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
  , _kafka_quota_balancer_min_shard_throughput_ratio(
      config::shard_local_cfg()
        .kafka_quota_balancer_min_shard_throughput_ratio.bind())
  , _kafka_quota_balancer_min_shard_throughput_bps(
      config::shard_local_cfg()
        .kafka_quota_balancer_min_shard_throughput_bps.bind())
  , _node_quota_default{calc_node_quota_default()}
  , _shard_quota{
      .in {node_to_shard_quota(_node_quota_default.in),
           _kafka_quota_balancer_window()},
      .eg {node_to_shard_quota(_node_quota_default.eg),
           _kafka_quota_balancer_window()}}
  , _probe(*this)
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
        // if the balancer is disabled, this is where the quotas are reset to
        // default. This needs to be called on every shard because the effective
        // balance is updated directly in this case.
        update_node_quota_default();
    });
    _kafka_quota_balancer_min_shard_throughput_ratio.watch(
      [this] { update_shard_quota_minimum(); });
    _kafka_quota_balancer_min_shard_throughput_bps.watch(
      [this] { update_shard_quota_minimum(); });

    if (ss::this_shard_id() == quota_balancer_shard) {
        _balancer_timer.set_callback([this] {
            ssx::spawn_with_gate(_balancer_gate, [this] {
                return quota_balancer_step().finally(
                  [this] { arm_balancer_timer(); });
            });
        });
    }
    _probe.setup_metrics();
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
    // Ignore _shard_quota_minimum because it only applies when quotas
    // are adjusted during balancing
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

void snc_quota_manager::record_request_receive(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_quota.in.use(request_size, now);
}

void snc_quota_manager::record_request_intake(
  const size_t request_size) noexcept {
    _probe.rec_traffic_in(request_size);
}

void snc_quota_manager::record_response(
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
        ingress_egress_state<std::optional<quota_t>> qold;
        if (_kafka_quota_balancer_node_period() == 0ms) {
            // set effective shard quotas to default shard quotas only if
            // the balancer is off. This resets all the uneven distribution of
            // effective quotas done by the balancer to the uniform  default
            // once the balancer is turned off, like if the node default quota
            // were not enabled
            qold = {std::nullopt, std::nullopt};
        } else {
            // the balancer is on, so the update will be calculated on shard0
            // and distributed between the shards synchronously to balancer runs
            // based on both old and new values for the default node quota
            qold = _node_quota_default;
        }

        // dependent update: effective shard quotas
        ssx::spawn_with_gate(
          _balancer_gate, [this, qold, qnew = new_node_quota_default] {
              return quota_balancer_update(qold, qnew);
          });
    }
    _node_quota_default = new_node_quota_default;
    // dependent update: shard minimum quota
    update_shard_quota_minimum();
}

void snc_quota_manager::update_shard_quota_minimum() {
    const auto f = [this](const std::optional<quota_t>& node_quota_default) {
        const auto shard_quota_default = node_to_shard_quota(
          node_quota_default);
        return std::max<quota_t>(
          _kafka_quota_balancer_min_shard_throughput_ratio()
            * shard_quota_default,
          _kafka_quota_balancer_min_shard_throughput_bps());
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
          != ss::lowres_clock::duration{}) {
            vlog(
              klog.warn,
              "qb - Quota balancer is invoked too often ({}), enforcing "
              "minimum sleep time. Consider increasing the balancer period.",
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
    _probe.rec_balancer_step();

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
    if (is_zero(borrowers_count)) {
        co_return;
    }
    vlog(
      klog.trace,
      "qb - Borrowers count: {}, borrowers:",
      borrowers_count,
      borrowers);

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
        split.in = std::div(collected.in, borrowers_count.in);
    }
    if (borrowers_count.eg > 0) {
        split.eg = std::div(collected.eg, borrowers_count.eg);
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

namespace detail {

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
            share.rem -= 1;
        } else if (share.rem < 0) {
            v -= 1;
            share.rem += 1;
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

/// If \p delta is negative, splits its value pro rata to \p quotas values and
/// into the slices limited by \p quota values. The slices are added to the
/// elements of \p schedule.
/// \pre sum(\p quotas) < abs(delta)
/// \pre schedule.size() == quotas.size()
/// \pre 0 <= quotas[i] for any i
void dispense_negative_deltas(
  std::vector<quota_t>& schedule, quota_t delta, std::vector<quota_t> quotas) {
    if (unlikely(schedule.size() != quotas.size())) {
        vlog(
          klog.error,
          "Schedule and quotas sizes must be equal. schedule: {}, quotas: {}",
          schedule,
          quotas);
        return;
    }
    if (delta >= 0) {
        return;
    }

    vlog(
      klog.trace,
      "qb - dispense_negative_deltas ( schedule: {}, quotas: {}, delta: {}",
      schedule,
      quotas,
      delta);

    const auto total_quotas = std::reduce(
      quotas.cbegin(), quotas.cend(), quota_t{0}, std::plus{});
    if (total_quotas != 0) {
        // dispense delta pro rate to quotas
        const double delta_d = delta;
        for (size_t k = 0; k != schedule.size(); ++k) {
            // cannot use integer arithmetic here because it is not uncommon for
            // all the multipliers and the divisor to be fairly large int64s
            quota_t share = std::llround(delta_d * quotas[k] / total_quotas);
            // share can happen to be slightly bigger than the remaining delta
            // at the last few iterations due to FP rounding, don't let it stay
            // so
            if (const quota_t overshare = std::abs(share) - std::abs(delta);
                overshare > 0) {
                if (unlikely(overshare > static_cast<quota_t>(k) + 1)) {
                    vlog(
                      klog.error,
                      "qb - share {} ≫ delta_remainder {}, delta: {}, quotas: "
                      "{}, quotas: {}, k: {}",
                      share,
                      delta,
                      delta_d,
                      quotas,
                      quotas,
                      k);
                }
                share = delta;
            }
            // limit share to the value of quota on the shard
            if (share < -quotas[k]) {
                share = -quotas[k];
            }
            quotas[k] -= std::abs(share);
            schedule[k] += share;
            delta -= share;
        }
    }

    // the remaining delta after this operation should not be larger than
    // schedule.size() because the amount has been dispensed
    // pro rata to the quotas, and the original delta is never bigger than
    // the sum of quotas
    if (delta == 0) {
        return;
    }
    if (unlikely(static_cast<uint64_t>(-delta) > schedule.size())) {
        vlog(klog.error, "qb - Remaining delta is too big: {}", delta);
    }

    // distribute the remaining delta equally between the shards that still have
    // some quota left
    const size_t quotas_left = std::count_if(
      quotas.cbegin(), quotas.cend(), [](const quota_t v) { return v > 0; });
    if (unlikely(quotas_left == 0)) {
        vlog(
          klog.error,
          "qb - No shards to distribute the remianing delta: {}",
          delta);
        return;
    }
    // shard share rounded to the floor (towards -inf)
    const quota_t share = (delta + 1) / static_cast<quota_t>(quotas_left) - 1;
    for (size_t k = 0; k != schedule.size(); ++k) {
        const auto d = std::max<quota_t>({share, delta, -quotas[k]});
        delta -= d;
        schedule[k] += d;
        quotas[k] += d;
    }
    if (delta != 0) {
        vlog(klog.error, "qb - Undispensed remaining delta: {}", delta);
    }
}

} // namespace detail
using namespace detail;

ss::future<> snc_quota_manager::quota_balancer_update(
  const ingress_egress_state<std::optional<quota_t>> old_node_quota_default,
  const ingress_egress_state<std::optional<quota_t>> new_node_quota_default) {
    const auto units = co_await _balancer_mx.get_units();
    _balancer_gate.check();
    vlog(
      klog.trace,
      "qb - Update: {} -> {}",
      old_node_quota_default,
      new_node_quota_default);

    // compare the old and the new default quotas, considering their presence
    // if both are present, the difference goes to `deltas` and will be
    // dispensed between shards, positive or negative
    // if only one is present, the new value goes towards `amounts`, preserving
    // presence information. `tristate` encapsulates that value adding the
    // 'disabled' state to that meaning that no change is required
    ingress_egress_state<quota_t> deltas{0, 0};
    ingress_egress_state<tristate<quota_t>> amounts;
    const auto calc_diff = [](
                             quota_t& delta,
                             tristate<quota_t>& amount,
                             const std::optional<quota_t>& qold,
                             const std::optional<quota_t>& qnew) {
        if (qold && qnew) {
            delta = *qnew - *qold;
        } else if (qold || qnew) {
            amount = tristate<quota_t>(qnew);
        }
    };
    calc_diff(
      deltas.in,
      amounts.in,
      old_node_quota_default.in,
      new_node_quota_default.in);
    calc_diff(
      deltas.eg,
      amounts.eg,
      old_node_quota_default.eg,
      new_node_quota_default.eg);

    // deliver shard quota updates of value type
    if (!amounts.in.is_disabled() || !amounts.eg.is_disabled()) {
        co_await container().invoke_on_all([amounts](snc_quota_manager& qm) {
            ingress_egress_state<std::optional<quota_t>> new_quota;
            if (!amounts.in.is_disabled()) {
                new_quota.in = node_to_shard_quota(amounts.in.get_optional());
            }
            if (!amounts.eg.is_disabled()) {
                new_quota.eg = node_to_shard_quota(amounts.eg.get_optional());
            }
            qm.maybe_set_quota(new_quota);
        });
    }

    // `deltas` are dispensed in full between the shards,
    // with an attempt of fairness by considering the current load
    if (is_zero(deltas)) {
        co_return;
    }

    // per-shard schedules of quota changes
    ingress_egress_state<std::vector<quota_t>> schedule{
      std::vector<quota_t>(ss::smp::count),
      std::vector<quota_t>(ss::smp::count)};
    // negative deltas are dispensed with consideration of the current load
    if (deltas.in < 0 || deltas.eg < 0) {
        // cap negative delta at -(total surrenderable quota),
        // difference between the required delta and the delta that can be
        // acquired goes towards the node deficit
        const auto quotas = co_await container().map(
          [](const snc_quota_manager& qm) {
              // minimum quota value for a shard is 1, remove that minimum
              // from the surrenderable quota amount reported
              return qm.get_quota()
                     - ingress_egress_state<quota_t>{
                       bottomless_token_bucket::min_quota,
                       bottomless_token_bucket::min_quota};
          });
        const auto total_quota = std::reduce(
          quotas.cbegin(),
          quotas.cend(),
          ingress_egress_state<quota_t>{0, 0},
          std::plus{});

        _node_deficit.in += cap_to_ceiling(deltas.in, -total_quota.in);
        _node_deficit.eg += cap_to_ceiling(deltas.eg, -total_quota.eg);
        if (!is_zero(_node_deficit)) {
            vlog(klog.trace, "qb - Node deficit: {}", _node_deficit);
        }

        auto quotas_soa = to_soa_layout(quotas);
        dispense_negative_deltas(
          schedule.in, deltas.in, std::move(quotas_soa.in));
        dispense_negative_deltas(
          schedule.eg, deltas.eg, std::move(quotas_soa.eg));
    }
    // postive deltas are disensed equally
    if (deltas.in > 0) {
        dispense_equally(schedule.in, deltas.in);
    }
    if (deltas.eg > 0) {
        dispense_equally(schedule.eg, deltas.eg);
    }

    vlog(klog.debug, "qb - Dispense delta updates {} as {}", deltas, schedule);

    // deliver `schedules` to the shards
    co_await container().invoke_on_all([&schedule](snc_quota_manager& qm) {
        qm.adjust_quota(
          {.in = schedule.in.at(ss::this_shard_id()),
           .eg = schedule.eg.at(ss::this_shard_id())});
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
    const auto f = [](
                     bottomless_token_bucket& shard_quota,
                     const quota_t delta,
                     const char* const side_name) {
        const quota_t new_quota = shard_quota.quota() + delta;
        if (unlikely(new_quota < bottomless_token_bucket::min_quota)) {
            vlog(
              klog.error,
              "qm - Adjust quota delta.{} {} would make quota {} less than "
              "minimum, capping at {}",
              side_name,
              delta,
              shard_quota.quota(),
              bottomless_token_bucket::min_quota);
            shard_quota.set_quota(bottomless_token_bucket::min_quota);
        } else {
            shard_quota.set_quota(new_quota);
        }
    };
    f(_shard_quota.in, delta.in, "in");
    f(_shard_quota.eg, delta.eg, "eg");
    vlog(klog.trace, "qm - Adjust quota: {} -> {}", delta, _shard_quota);
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
struct fmt::formatter<std::ldiv_t> : parseless_formatter {
    template<typename Ctx>
    auto format(const std::ldiv_t& v, Ctx& ctx) const {
        return fmt::format_to(ctx.out(), "(q:{}, r:{})", v.quot, v.rem);
    }
};
