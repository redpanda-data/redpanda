// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/quota_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "prometheus/prometheus_sanitize.h"
#include "vlog.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/thread.hh>

#include <fmt/chrono.h>

#include <chrono>

using namespace std::chrono_literals;

namespace kafka {
using clock = quota_manager::clock;
using throttle_delay = quota_manager::throttle_delay;

void throughput_quotas_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};
    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:tpquotas"),
      {
        sm::make_counter(
          "balancer_runs",
          [this] { return _balancer_runs; },
          sm::description(
            "{}: Number of times throughput quota balancer has been run"))
          .aggregate(aggregate_labels),
      });
}

std::ostream& operator<<(std::ostream& o, const throughput_quotas_probe& p) {
    o << "{"
      << "balancer_runs: " << p._balancer_runs << "}";
    return o;
}

struct abort_on_configuration_change {};
struct abort_on_stop {};

quota_manager::quota_manager()
  : _default_num_windows(config::shard_local_cfg().default_num_windows.bind())
  , _default_window_width(config::shard_local_cfg().default_window_sec.bind())
  , _default_target_produce_tp_rate(
      config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_fetch_byte_rate_quota.bind())
  , _kafka_throughput_limit_node_in_bps(
      config::shard_local_cfg().kafka_throughput_limit_node_in_bps.bind())
  , _kafka_throughput_limit_node_out_bps(
      config::shard_local_cfg().kafka_throughput_limit_node_out_bps.bind())
  , _kafka_quota_balancer_window(
      config::shard_local_cfg().kafka_quota_balancer_window.bind())
  , _kafka_quota_balancer_node_period(
      config::shard_local_cfg().kafka_quota_balancer_node_period.bind())
  , _shard_ingress_quota(
      get_shard_ingress_quota_default(), _kafka_quota_balancer_window())
  , _shard_egress_quota(
      get_shard_egress_quota_default(), _kafka_quota_balancer_window())
  , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
  , _max_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
    _gc_timer.set_callback([this] {
        auto full_window = _default_num_windows() * _default_window_width();
        gc(full_window);
    });
    // _balancer_timer.set_callback([this] {
    //     if (_as.abort_requested()) {
    //         return;
    //     }
    //     maybe_arm_balancer_timer();
    //     quota_balancer_step();
    // });

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
    _kafka_quota_balancer_node_period.watch([this] {
        notify_kafka_quota_balancer_node_period_change();
        // if (_balancer_timer.armed() || _as.abort_requested()) {
        //     return;
        // }
        // maybe_arm_balancer_timer();
    });
next steps: bisect the crash, wait for the balancer thread to join in stop
    //_probe.setup_metrics();
}

quota_manager::~quota_manager() {
    _gc_timer.cancel();
    _balancer_timer.cancel();
}

ss::future<> quota_manager::stop() {
    _as.request_abort_ex(abort_on_stop{});
    _gc_timer.cancel();
    _balancer_timer.cancel();
    return ss::make_ready_future<>();
}

ss::future<> quota_manager::start() {
    static constexpr ss::shard_id balancer_shard = 0;
    _gc_timer.arm_periodic(_gc_freq);
    if (ss::this_shard_id() == balancer_shard) {
        ssx::background = ss::async([this] { quota_balancer(); });
    }
    // maybe_arm_balancer_timer();
    return ss::make_ready_future<>();
}

/*
 * Per-Client and Per-Client-Group Quotas
 */

quota_manager::client_quotas_t::iterator
quota_manager::maybe_add_and_retrieve_quota(
  const std::optional<std::string_view>& quota_id,
  const clock::time_point& now) {
    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    auto qid = quota_id ? *quota_id : "";

    // find or create the throughput tracker for this client
    //
    // c++20: heterogeneous lookup for unordered_map can avoid creation of
    // an sstring here but std::unordered_map::find isn't using an
    // equal_to<> overload. this is a general issue we'll be looking at. for
    // now, these client-name strings are small. This will be solved in
    // c++20 via Hash::transparent_key_equal.
    auto [it, inserted] = _client_quotas.try_emplace(
      ss::sstring(qid),
      client_quota{
        now,
        clock::duration(0),
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        /// pm_rate is only non-nullopt on the qm home shard
        (ss::this_shard_id() == quota_manager_shard)
          ? std::optional<token_bucket_rate_tracker>(
            {*_target_partition_mutation_quota(),
             static_cast<uint32_t>(_default_num_windows()),
             _default_window_width()})
          : std::optional<token_bucket_rate_tracker>()});

    // bump to prevent gc
    if (!inserted) {
        it->second.last_seen = now;
    }

    return it;
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
static std::optional<std::string_view> get_client_quota_id(
  const std::optional<std::string_view>& client_id,
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    group_quota) {
    if (!client_id) {
        return std::nullopt;
    }
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return group_and_limit.first;
        }
    }
    return client_id;
}

int64_t quota_manager::get_client_target_produce_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_produce_tp_rate();
    }
    auto group_tp_rate = _target_produce_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_produce_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_produce_tp_rate();
}

std::optional<int64_t> quota_manager::get_client_target_fetch_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_fetch_tp_rate();
    }
    auto group_tp_rate = _target_fetch_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_fetch_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_fetch_tp_rate();
}

ss::future<std::chrono::milliseconds> quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
    if (!_target_partition_mutation_quota()) {
        co_return 0ms;
    }
    co_return co_await container().invoke_on(
      quota_manager_shard, [client_id, mutations, now](quota_manager& qm) {
          return qm.do_record_partition_mutations(client_id, mutations, now);
      });
}

std::chrono::milliseconds quota_manager::do_record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    vassert(
      ss::this_shard_id() == quota_manager_shard,
      "This method can only be executed from quota manager home shard");

    auto quota_id = get_client_quota_id(client_id, {});
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    const auto units = it->second.pm_rate->record_and_measure(mutations, now);
    auto delay_ms = 0ms;
    if (units < 0) {
        /// Throttle time is defined as -K * R, where K is the number of
        /// tokens in the bucket and R is the avg rate. This only works when
        /// the number of tokens are negative which is the case when the
        /// rate limiter recommends throttling
        const auto rate = (units * -1)
                          * std::chrono::seconds(
                            *_target_partition_mutation_quota());
        delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(rate);
        std::chrono::milliseconds max_delay_ms(_max_delay());
        if (delay_ms > max_delay_ms) {
            vlog(
              klog.info,
              "Found partition mutation rate for window of: {}. Client:{}, "
              "Estimated backpressure delay of {}. Limiting to {} backpressure "
              "delay",
              rate,
              it->first,
              delay_ms,
              max_delay_ms);
            delay_ms = max_delay_ms;
        }
    }
    return delay_ms;
}

static std::chrono::milliseconds calculate_delay(
  double rate, uint32_t target_rate, clock::duration window_size) {
    std::chrono::milliseconds delay_ms(0);
    if (rate > target_rate) {
        auto diff = rate - target_rate;
        double delay = (diff / target_rate)
                       * static_cast<double>(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                           window_size)
                           .count());
        delay_ms = std::chrono::milliseconds(static_cast<uint64_t>(delay));
    }
    return delay_ms;
}

std::chrono::milliseconds quota_manager::throttle(
  std::optional<std::string_view> quota_id,
  uint32_t target_rate,
  const clock::time_point& now,
  rate_tracker& rate_tracker) {
    auto rate = rate_tracker.measure(now);
    auto delay_ms = calculate_delay(
      rate, target_rate, rate_tracker.window_size());

    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, "
          "Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          rate,
          quota_id,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }
    return delay_ms;
}

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_produce_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_produce_tp_rate_per_client_group());
    auto it = maybe_add_and_retrieve_quota(quota_id, now);

    it->second.tp_produce_rate.record(bytes, now);
    auto target_tp_rate = get_client_target_produce_tp_rate(quota_id);
    auto delay_ms = throttle(
      quota_id, target_tp_rate, now, it->second.tp_produce_rate);
    auto prev = it->second.delay;
    it->second.delay = delay_ms;
    throttle_delay res{};
    res.enforce = prev.count() > 0;
    res.duration = it->second.delay;
    return res;
}

void quota_manager::record_fetch_tp(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    it->second.tp_fetch_rate.record(bytes, now);
}

throttle_delay quota_manager::throttle_fetch_tp(
  std::optional<std::string_view> client_id, clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto target_tp_rate = get_client_target_fetch_tp_rate(quota_id);

    if (!target_tp_rate) {
        return {};
    }
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    it->second.tp_fetch_rate.maybe_advance_current(now);
    auto delay_ms = throttle(
      quota_id, *target_tp_rate, now, it->second.tp_fetch_rate);
    throttle_delay res{};
    res.enforce = true;
    res.duration = delay_ms;
    return res;
}

// erase inactive tracked quotas. windows are considered inactive if
// they have not received any updates in ten window's worth of time.
void quota_manager::gc(clock::duration full_window) {
    auto now = clock::now();
    auto expire_age = full_window * 10;
    // c++20: replace with std::erase_if
    absl::erase_if(
      _client_quotas,
      [now, expire_age](const std::pair<ss::sstring, client_quota>& q) {
          return (now - q.second.last_seen) > expire_age;
      });
}

/*
 * Shard Global Quotas
 */

namespace {

bottomless_token_bucket::quota_t get_default_quota_from_property(
  const config::binding<std::optional<uint64_t>>& prop) {
    if (prop()) {
        const uint64_t v = *prop() / ss::smp::count;
        if (v >= static_cast<uint64_t>(bottomless_token_bucket::max_quota)) {
            return bottomless_token_bucket::max_quota;
        }
        if (v < 1) {
            return 1;
        }
        return static_cast<bottomless_token_bucket::quota_t>(v);
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

quota_manager::shard_quota_t
quota_manager::get_shard_ingress_quota_default() const {
    const shard_quota_t v = get_default_quota_from_property(
      _kafka_throughput_limit_node_in_bps);
    vlog(klog.debug, "Default shard TP ingress quota: {}", v);
    return v;
}

quota_manager::shard_quota_t
quota_manager::get_shard_egress_quota_default() const {
    const shard_quota_t v = get_default_quota_from_property(
      _kafka_throughput_limit_node_out_bps);
    vlog(klog.debug, "Default shard TP egress quota {}", v);
    return v;
}

quota_manager::shard_delays_t quota_manager::get_shard_delays(
  clock::time_point& connection_throttle_until,
  const clock::time_point now) const {
    shard_delays_t res;

    // force throttle whatever the client did not do on its side
    if (now < connection_throttle_until) {
        res.enforce = connection_throttle_until - now;
    }

    // throttling delay the connection should be requested to throttle
    // this time
    res.request = std::min(
      _max_delay(),
      std::max(
        eval_delay(_shard_ingress_quota), eval_delay(_shard_egress_quota)));
    connection_throttle_until = now + res.request;

    return res;
}

void quota_manager::record_request_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_ingress_quota.use(request_size, now);
}

void quota_manager::record_response_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_egress_quota.use(request_size, now);
}

void quota_manager::maybe_arm_balancer_timer() {
    if (
      _kafka_quota_balancer_node_period()
      != std::chrono::milliseconds::zero()) {
        _balancer_timer.arm(
          ss::steady_clock_type::now() + _kafka_quota_balancer_node_period());
    }
}

void quota_manager::notify_kafka_quota_balancer_node_period_change() {
    _as.request_abort_ex(abort_on_configuration_change{});
    // replace abort source immediately so that only subscribers are aborted now
    // and to enable future aborts
    _as = ss::abort_source();
}

void quota_manager::quota_balancer() {
    vlog(klog.debug, "Quota balancer started");
    using clock = ss::steady_clock_type;
    static constexpr auto min_sleep_time = 1ms; // TBD config?

    clock::time_point next_invocation = clock::now()
                                        + _kafka_quota_balancer_node_period();
    try {
        for (;;) {
            auto sleep_time = next_invocation - clock::now();
            if (sleep_time < min_sleep_time) {
                vlog(
                  klog.warn,
                  "Quota balancer is invoked too often ({}), enforcing minimum "
                  "sleep time",
                  sleep_time);
                sleep_time = min_sleep_time;
            }
            const bool sleep_succeeded
              = ss::sleep_abortable(sleep_time, _as)
                  .then([] { return true; })
                  .handle_exception_type(
                    [](const abort_on_configuration_change&) { return false; })
                  .get0();
            next_invocation = clock::now()
                              + _kafka_quota_balancer_node_period();

            if (sleep_succeeded) {
                quota_balancer_step().get0();
            }
        }
    } catch (const abort_on_stop&) {
        // TBD: remove
        vlog(klog.debug, "Quota balancer stopped by aborting the sleep");
    }
    vlog(klog.info, "Quota balancer stopped");
}

ss::future<> quota_manager::quota_balancer_step() {
    vlog(klog.trace, "Quota balancer step");
    _probe.balancer_run();

    // determine the borrowers and whether any balancing is needed now
    //_quota_manager.invoke_on_all
    return ss::make_ready_future();
}

} // namespace kafka
