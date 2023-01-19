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
#include <fmt/ranges.h>

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
}

quota_manager::~quota_manager() {
    _gc_timer.cancel();
}

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    return _snc_qm.stop();
}

ss::future<> quota_manager::start() {
    _gc_timer.arm_periodic(_gc_freq);
    _snc_qm.start(container());
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
 * Shard/Node/Cluster Global Quotas
 */

void snc_quota_manager::collect_dispense_amount_t::append ( const snc_quota_manager::collect_dispense_amount_t& rhs ) noexcept {
  if ( rhs.amount_is == value ) {
    *this = rhs;
  } else if ( amount_is == value ) {
    // keep the existing value
  } else {
    amount += rhs.amount;
  }
}

snc_quota_manager::snc_quota_manager()
:  _max_kafka_throttle_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind())
  , _kafka_throughput_limit_node_bps{{
      config::shard_local_cfg().kafka_throughput_limit_node_in_bps.bind(),
      config::shard_local_cfg().kafka_throughput_limit_node_out_bps.bind()}}
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
  , _shard_quota(
    to_each([this](const quota_t quota){
      return bottomless_token_bucket(quota, _kafka_quota_balancer_window());
    }, get_shard_quota_default())
  )
{
    update_shard_quota_minimum();
    _kafka_throughput_limit_node_bps.in().watch([this] {
        update_node_quota_default();
    });
    _kafka_throughput_limit_node_bps.out().watch([this] {
        update_node_quota_default();
    });
    _kafka_quota_balancer_window.watch([this] {
        const auto v = _kafka_quota_balancer_window();
        vlog(klog.debug, "snc_qm - Set shard TP token bucket window: {}", v);
        _shard_quota.in().set_width(v);
        _shard_quota.out().set_width(v);
    });
    _kafka_quota_balancer_node_period.watch([this] {
        notify_quota_balancer_of_config_change();
    });
    _kafka_quota_balancer_min_shard_thoughput_ratio.watch(
      [this] { update_shard_quota_minimum(); });
    _kafka_quota_balancer_min_shard_thoughput_bps.watch(
      [this] { update_shard_quota_minimum(); });
    _probe.setup_metrics();
}

snc_quota_manager::~snc_quota_manager() noexcept {
    if (!_as.abort_requested()) {
      _as.request_abort_ex(abort_on_stop{});
    }
}

void snc_quota_manager::start(ss::sharded<quota_manager>& container) {
    _container = &container;
    if (ss::this_shard_id() == quota_manager_shard) {
        _balancer_thread = ss::thread([this] { quota_balancer(); });
    }
    // maybe_arm_balancer_timer();
}

ss::future<> snc_quota_manager::stop() {
    _as.request_abort_ex(abort_on_stop{});
    // _balancer_timer.cancel();
    if (ss::this_shard_id() == quota_manager_shard) {
        return _balancer_thread.join();
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

snc_quota_manager::quota_t node_to_shard_quota(
  const std::optional<snc_quota_manager::quota_t> node_quota) {
    if (node_quota) {
      const snc_quota_manager::quota_t v = *node_quota / ss::smp::count;
      if (v < 1) {
          return snc_quota_manager::quota_t{1};
      }
      return v;
    } else {
          return bottomless_token_bucket::max_quota;
    }
}

bool is_zero ( const inoutpair<snc_quota_manager::quota_t>& v ) noexcept {
  return v.in() == 0 && v.out() == 0;
}

} // namespace

inoutpair<std::optional<snc_quota_manager::quota_t>>
snc_quota_manager::calc_node_quota_default() const {
    const auto default_quota = to_each(
      [](const config::binding<std::optional<quota_t>>& tpl_node_bps)
        -> std::optional<quota_t> {
          // here will be the code to merge node limit 
          // and node share of cluster limit; so far it's node limit only
          return tpl_node_bps();
      },
      _kafka_throughput_limit_node_bps);
    vlog(klog.debug, "snc_qm - Default node TP quotas: {}", default_quota);
    return default_quota;
}

inoutpair<snc_quota_manager::quota_t>
snc_quota_manager::get_shard_quota_default() const {
    const auto default_quota = to_each(
      // [](const std::optional<quota_t>& node_quota_default) {
      //     return node_to_shard_quota(node_quota_default);
      // },
      &node_to_shard_quota,
      _node_quota_default);
    vlog(klog.debug, "snc_qm - Default shard TP quotas: {}", default_quota);
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
      std::max(eval_delay(_shard_quota.in()), eval_delay(_shard_quota.out())));
    connection_throttle_until = now + res.request;

    return res;
}

void snc_quota_manager::record_request_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_quota.in().use(request_size, now);
}

void snc_quota_manager::record_response_tp(
  const size_t request_size, const clock::time_point now) noexcept {
    _shard_quota.out().use(request_size, now);
}


void snc_quota_manager::notify_quota_balancer_of_config_change() {
    _as.request_abort_ex(abort_on_configuration_change{});
    // replace abort source immediately so that only subscribers are aborted now
    // and to enable future aborts
    _as = ss::abort_source();
}

std::chrono::milliseconds
snc_quota_manager::get_quota_balancer_node_period() const {
    const auto v = _kafka_quota_balancer_node_period();
    // zero period in config means do not run balancer
    if (v == std::chrono::milliseconds::zero()) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          ss::steady_clock_type::duration::max() / 2);
    }
    return v;
}

void snc_quota_manager::update_node_quota_default() {
    const inoutpair<std::optional<quota_t>> new_node_quota_default
      = calc_node_quota_default();
    if (ss::this_shard_id() == quota_manager_shard) {
        // downstream updates:
        // - shard effective quota (via _shard_quotas_update):
        to_each(
          [](
            collect_dispense_amount_t& shard_quotas_update,
            const std::optional<quota_t>& qold,
            const std::optional<quota_t>& qnew) {
              collect_dispense_amount_t diff;
              if (qold && qnew) {
                  diff.amount = *qnew - *qold;
                  diff.amount_is = collect_dispense_amount_t::delta;
              } else if (qold || qnew) {
                  diff.amount = node_to_shard_quota(qnew);
                  diff.amount_is = collect_dispense_amount_t::value;
              }
              shard_quotas_update.append(diff);
          },
          _shard_quotas_update,
          _node_quota_default,
          new_node_quota_default);
    }
    _node_quota_default = new_node_quota_default;
    // - shard minimum quota:
    update_shard_quota_minimum();
    // apply the configuration change to the balancer
    notify_quota_balancer_of_config_change();
}

void snc_quota_manager::update_shard_quota_minimum() {
  _shard_quota_minimum = to_each ( [this](const std::optional<quota_t>& node_quota_default) {
    const auto shard_quota_default = node_to_shard_quota(node_quota_default);
    return std::max<quota_t> (
      _kafka_quota_balancer_min_shard_thoughput_ratio() * shard_quota_default,
      _kafka_quota_balancer_min_shard_thoughput_bps()
    );
  }, _node_quota_default );
  // downstream updates: none
}

void snc_quota_manager::quota_balancer() {
    vlog(klog.debug, "snc_qm - Quota balancer started");
    using clock = ss::steady_clock_type;
    static constexpr auto min_sleep_time = 2ms; // TBD config?

    bool sleep_succeeded = false;
    clock::time_point next_invocation = clock::now()
                                        + get_quota_balancer_node_period();
    try {
        for (;;) {
            const auto prev_invocation = clock::now();
            auto sleep_time = next_invocation - prev_invocation;
            if (sleep_succeeded && sleep_time < min_sleep_time) {
                // negative sleep_time occurs when balancer takes to run
                // longer than the balancer period
                vlog(
                  klog.warn,
                  "snc_qm - Quota balancer is invoked too often ({}), "
                  "enforcing minimum sleep time",
                  sleep_time);
                sleep_time = min_sleep_time;
            }
            sleep_succeeded = ss::sleep_abortable(sleep_time, _as)
                  .then([] { return true; })
                  .handle_exception_type(
                    [](const abort_on_configuration_change&) { return false; })
                  .get0();

            const auto now = clock::now();
            if (unlikely(
                  !_shard_quotas_update.in().empty()
                  || !_shard_quotas_update.out().empty())) {
                quota_balancer_update().get0();
            }
            if (likely(sleep_succeeded)) {
                next_invocation = now
                              + get_quota_balancer_node_period();
                quota_balancer_step().get0();
            } else {
                next_invocation = prev_invocation
                                  + get_quota_balancer_node_period();
            }
        }
    } catch (const abort_on_stop&) {
    }
    vlog(klog.info, "snc_qm - Quota balancer stopped");
}

using shard_count_t = boost::uint_t<std::numeric_limits<ss::shard_id>::digits>::fast;

struct borrower_t {
  ss::shard_id shard_id;
  inoutpair<shard_count_t> is_borrower;
};

ss::future<> snc_quota_manager::quota_balancer_step() {
    vlog(klog.trace, "snc_qm - Quota balancer step");
    _probe.balancer_run();

    // determine the borrowers and whether any balancing is needed now
    const std::vector<borrower_t> borrowers = co_await _container->map(
      [](quota_manager& qm) -> borrower_t {
          return {
              ss::this_shard_id(), { to_each(
                                     [](const quota_t d) -> size_t {
                                         return d > 0 ? 1u : 0u;
                                     },
                                     qm.snc_qm().get_deficiency()) }
          };
      });
    vlog(klog.trace, "snc_qm - Quota balancer: borrowers: {}", borrowers);

    const auto borrowers_count = std::accumulate(
      borrowers.cbegin(),
      borrowers.cend(),
      inoutpair<shard_count_t>{{0u, 0u}},
      [](const inoutpair<shard_count_t>& s, const borrower_t& b) {
          return to_each(std::plus{}, s, b.is_borrower);
      });
    vlog(
      klog.trace,
      "snc_qm - Quota balancer: borrowers count: {}", borrowers_count );

    // collect quota from lenders
    const inoutpair<quota_t> collected = co_await _container->map_reduce0(
      [borrowers_count](quota_manager& qm) {
          const auto to_collect = to_each(
            [](
              const quota_t surplus,
              const shard_count_t borrowers_count) {
                return borrowers_count > 0 ? surplus / 2 : 0;
            },
            qm.snc_qm().get_surplus(),
            borrowers_count);
          qm.snc_qm().adjust_quota(to_each(std::negate{},to_collect));
          return to_collect;
      },
      inoutpair<quota_t>{{0, 0}},
      [](const inoutpair<quota_t>& s, const inoutpair<quota_t>& v) {
          return to_each ( std::plus{}, s, v );
      });
    vlog(klog.trace, "snc_qm - Quota balancer: collected: {}", collected);

    // dispense the collected amount among the borrowers
    auto split = to_each(
      [](
        const quota_t collected,
        const shard_count_t borrowers_count) {
          if ( borrowers_count == 0 ) {
            vassert(collected==0, "Quota collected when borrowers are absent");
            return std::div(collected, shard_count_t{1});
          }
          return std::div(collected, borrowers_count);
      },
      collected,
      borrowers_count);
    for ( const borrower_t& b : borrowers ) {
      if ( !b.is_borrower.in() && !b.is_borrower.out() ) {
        continue;
      }
      const inoutpair<quota_t> share = to_each([]( auto& split, const bool is_borrower ) -> quota_t {
        if ( !is_borrower ) {
          return 0;
        }
        if ( split.rem == 0 ) {
          return split.quot;
        }
        --split.rem;
        return split.quot + 1;
      }, split, b.is_borrower );
      // TBD: make the invocation parallel
      co_await _container->invoke_on ( b.shard_id, [share](quota_manager& qm) {
        qm.snc_qm().adjust_quota(share);
      });
    }
}

ss::future<> snc_quota_manager::quota_balancer_update() {
    vlog(klog.trace, "snc_qm - Quota balancer update: {}", _shard_quotas_update);

    // deliver shard quota updates of value type
    co_await _container->invoke_on_all([this](quota_manager& qm) {
        to_each(
          [](bottomless_token_bucket& shard_quota, const collect_dispense_amount_t& shard_quotas_update) {
              if (
                shard_quotas_update.amount_is
                == collect_dispense_amount_t::value) {
                  shard_quota.set_quota(
                    shard_quotas_update.amount);
              }
          },
          qm.snc_qm()._shard_quota, _shard_quotas_update );
    });

    // deliver deltas and try to dispense them fairly and in full
    inoutpair<quota_t> deltas = to_each(
      [](const collect_dispense_amount_t& shard_quotas_update) {
          if (
            shard_quotas_update.amount_is == collect_dispense_amount_t::delta) {
              return shard_quotas_update.amount;
          }
          return quota_t{0};
      },
      _shard_quotas_update);

    if (!is_zero(deltas)) {

      inoutpair<std::vector<quota_t>> schedule {{ std::vector<quota_t>(ss::smp::count), std::vector<quota_t>(ss::smp::count) }};

      if (deltas.in() < 0 || deltas.out() < 0 ) {

        // cap negative delta at -(total surrenderable quota), 
        // diff towards node deficit
        const auto total_quota = co_await _container->map_reduce0 ( 
          [](const quota_manager& qm) {
            return to_each(std::minus{}, qm.snc_qm().get_quota(), qm.snc_qm()._shard_quota_minimum);
          },
          inoutpair<quota_t> {{ 0, 0 }},
          [](const inoutpair<quota_t>& s, const inoutpair<quota_t>& v) {
            return to_each(std::plus{}, s, v);
          } );
        to_each ([](quota_t& delta, quota_t& node_deficit, const quota_t total_quota) {
          if ( const quota_t d = -delta - total_quota; d > 0 ) {
            node_deficit += d;
            delta += d;
          }
        }, deltas, _node_deficit, total_quota );

        const auto surplus = co_await _container->map ( 
          [](const quota_manager& qm) {
            return qm.snc_qm().get_surplus();
          } );
        const auto total_surplus = std::reduce ( surplus.cbegin(), surplus.cend(),
          inoutpair<quota_t> {{ 0, 0 }},
          [](const inoutpair<quota_t>& lhs, const inoutpair<quota_t>& rhs) {
            return to_each(std::plus{}, lhs, rhs);
          } );
        to_each([](std::vector<quota_t>& schedule, quota_t& delta, const quota_t total_surplus, const std::vector<quota_t>& surplus){
          if ( delta < 0 ) {
            if ( delta > -total_surplus ) {
                quota_t remainder = delta;
                // pro rata to surpluses
                for ( size_t k=0; k!=ss::smp::count; ++k ) {
                  const quota_t share = muldiv ( delta, surplus.at(k), total_surplus );
                  schedule.at(k) += share;
                  remainder -= share;
                }
                // the remainder equally
                vassert ( remainder <= 0, "Expected {} <= 0; delta: {}, total_surplus: {}, surpluses: {}",
                  remainder, delta, total_surplus, surplus );
                if ( remainder < 0 ) {
                  const quota_t d = (remainder+1) / schedule.size() - 1;
                  for ( quota_t& s : schedule ) {
                    const quota_t dd = std::max(d,remainder);
                    remainder -= dd;
                    s += dd;
                  }
                }
            } else { // delta <= -total_surplus
                // all surpluses
                for ( size_t k=0; k!=ss::smp::count; ++k ) {
                    const quota_t share = -surplus.at(k);
                    schedule.at(k) += share;
                    delta -= share;
                }
                // the rest equally
                auto share = std::div(delta, ss::smp::count);
                for ( quota_t& s : schedule ) {
                  s += share.quot;
                  if ( share.rem < 0 ) {
                    s += -1;
                    share.quot -= -1;
                  }
                } 
            }
          } 
        }, schedule, deltas, total_surplus, to_inside_out(surplus) );

      }
      if (deltas.in() > 0 || deltas.out() > 0 ) {

        to_each([](std::vector<quota_t>& schedule, quota_t& delta){
          if ( delta > 0 ) {
            // equally
            auto share = std::div(delta, ss::smp::count);
            for ( quota_t& s : schedule ) {
              s += share.quot;
              if ( share.rem > 0 ) {
                s += 1;
                share.quot -= 1;
              }
            } 
          }
        }, schedule, deltas );

      }

      vlog ( klog.debug, "snc_qm - Quota balancer dispense delta updates {} of {} as {}",
        deltas, _shard_quotas_update, schedule );

      co_await _container->invoke_on_all ( [&schedule](quota_manager& qm) {
        qm.snc_qm().adjust_quota ( 
          to_each ( [](const std::vector<quota_t>& schedule) {
            return schedule.at(ss::this_shard_id());
          }, schedule ) );
      } );

    }
    // the update has been applied
    _shard_quotas_update = {{{},{}}};
}

void snc_quota_manager::adjust_quota(const inoutpair<quota_t>& delta) noexcept {
    to_each([](bottomless_token_bucket& b, const quota_t delta) {
        b.set_quota(b.quota() + delta);
    }, _shard_quota, delta);
    vlog(klog.trace, "snc_qm - adjust_quota: {} -> {}", delta, _shard_quota);
}

} // namespace kafka

struct parseless_formatter {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        return ctx.begin();
    }
};

template<class T>
struct fmt::formatter<kafka::inoutpair<T>> : parseless_formatter {
    template<typename Ctx>
    auto format(const kafka::inoutpair<T>& p, Ctx& ctx) const {
        return fmt::format_to(
          ctx.out(), "(i:{}, o:{})", p.in(), p.out());
    }
};

template<>
struct fmt::formatter<kafka::borrower_t> : parseless_formatter {
    template<typename Ctx>
    auto format(const kafka::borrower_t& v, Ctx& ctx) const {
      return fmt::format_to(ctx.out(), "{{{}, {}}}", v.shard_id, v.is_borrower);
    }
};

template<>
struct fmt::formatter<kafka::snc_quota_manager::collect_dispense_amount_t> : parseless_formatter {
    template<typename Ctx>
    auto format(const kafka::snc_quota_manager::collect_dispense_amount_t& v, Ctx& ctx) const {
      const char* ai_name;
      switch(v.amount_is) {
        case kafka::snc_quota_manager::collect_dispense_amount_t::delta: ai_name = "delta"; break;
        case kafka::snc_quota_manager::collect_dispense_amount_t::value: ai_name = "value"; break;
        default: ai_name = "?"; break;
      }
      return fmt::format_to(ctx.out(), "{{{}: {}}}", ai_name, v.amount );
    }
};
