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
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "kafka/server/token_bucket_rate_tracker.h"
#include "resource_mgmt/rate.h"
#include "seastarx.h"
#include "utils/bottomless_token_bucket.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>
#include <string_view>
#include <deque>

namespace kafka {

// Shard on which partition mutation rate metrics are aggregated on
static constexpr ss::shard_id quota_manager_shard = 0;

class throughput_quotas_probe {
public:
    void balancer_run() { ++_balancer_runs; }

    void setup_metrics();
    // void setup_public_metrics();

    uint32_t get_balancer_runs() const noexcept { return _balancer_runs; }

private:
    ss::metrics::metric_groups _metrics;
    uint32_t _balancer_runs = 0;
    friend std::ostream&
    operator<<(std::ostream& o, const throughput_quotas_probe& p);
};

template<class T>
struct inoutpair : std::pair<T, T> {
    using pair_t = std::pair<T, T>;
    T& in() noexcept { return pair_t::first; }
    const T& in() const noexcept { return pair_t::first; }
    T& out() noexcept { return pair_t::second; }
    const T& out() const noexcept { return pair_t::second; }
};

template <typename T> concept arithmetic = std::is_arithmetic_v<T>;

template<arithmetic T>
struct inoutpair<T> : std::pair<T, T> {
    using pair_t = std::pair<T, T>;
    T& in() noexcept { return pair_t::first; }
    T in() const noexcept { return pair_t::first; }
    T& out() noexcept { return pair_t::second; }
    T out() const noexcept { return pair_t::second; }
};

template<>
struct inoutpair<void> {};

namespace detail{
template<class T> struct unwrap_inoutpair {};

template<class U>
struct unwrap_inoutpair<inoutpair<U>> {
  using type = U;
};

template<class U>
struct unwrap_inoutpair<const inoutpair<U>> {
  using type = const U;
};

template<class U>
struct unwrap_inoutpair<inoutpair<U>&> {
  using type = U&;
};

template<class U>
struct unwrap_inoutpair<const inoutpair<U>&> {
  using type = const U&;
};

template<class T> using unwrap_inoutpair_t = typename unwrap_inoutpair<T>::type;

}

template<
  class... T,
  std::invocable<detail::unwrap_inoutpair_t<T>...> F,
  class TRes = std::invoke_result_t<F,detail::unwrap_inoutpair_t<T>...>
  >
inoutpair<TRes> to_each(F f, /*const inoutpair<T>&... args*/ T&&... args) {
  if constexpr (std::is_void_v<TRes>) {
    std::invoke(f, args.in()...);
    std::invoke(f, args.out()...);
    return {};
  } else {
    return inoutpair<TRes>{
      {std::invoke(f, args.in()...), 
      std::invoke(f, args.out()...)}};
  }
}

template<class T>
inoutpair<std::vector<T>> to_inside_out ( const std::vector<inoutpair<T>>& v ) {
  inoutpair<std::vector<T>> res;
  res.in().reserve(v.size());
  res.out().reserve(v.size());
  for ( const inoutpair<T>& i : v ) {
    res.in().push_back( i.in() );
    res.out().push_back( i.out() );
  };
  return res;
}


/// Isolates \ref quota_manager functionality related to 
/// shard/node/cluster (SNC) wide quotas and limits
class snc_quota_manager {
public:
    using clock = ss::lowres_clock;
    using quota_t = bottomless_token_bucket::quota_t;

    snc_quota_manager();
    snc_quota_manager(const snc_quota_manager&) = delete;
    snc_quota_manager& operator=(const snc_quota_manager&) = delete;
    snc_quota_manager(snc_quota_manager&&) = delete;
    snc_quota_manager& operator=(snc_quota_manager&&) = delete;
    ~snc_quota_manager() noexcept;

    void start(ss::sharded<class quota_manager>&);
    ss::future<> stop();

    /// @p enforce delay to enforce in this call
    /// @p request delay to request from the client via throttle_ms
    struct delays_t {
        clock::duration enforce{0};
        clock::duration request{0};
    };

    /// Determine throttling required by shard level TP quotas.
    /// @param connection_throttle_until (in,out) until what time the client
    /// on this conection should throttle until. If it does not, this throttling
    /// will be enforced on the next call. In: value from the last call, out:
    /// value saved until the next call.
    delays_t get_shard_delays(
      clock::time_point& connection_throttle_until,
      clock::time_point now) const;

    void record_request_tp(
      size_t request_size, clock::time_point now) noexcept;

    void record_response_tp(
      size_t request_size, clock::time_point now) noexcept;

    const throughput_quotas_probe&
    get_throughput_quotas_probe() const noexcept {
        return _probe;
    };

private:
    /// Describes an instruction to the balancer how to alter effective shard quotas
    /// When \p amount_is == \p delta, \p amount should be dispensed among shard 
    /// quotas (if positive) or collected from shard quotas (if negative).
    /// When \p amount_is == \p value, \p amount contains the new shard quota value
    /// and each shard quota should be reset to that value regardless of what is 
    /// there now.
    struct collect_dispense_amount_t {
      snc_quota_manager::quota_t amount {0};
      enum { delta, value } amount_is{delta};
      bool empty() const noexcept { return amount == 0 && amount_is==delta; }
      void append ( const collect_dispense_amount_t& rhs ) noexcept;
    };
    friend struct fmt::formatter<collect_dispense_amount_t>;

    // Returns value based on upstream values, not the _node_quota_default
    inoutpair<std::optional<quota_t>> calc_node_quota_default() const;
    // Uses the func above to update _node_quota_default and dependencies
    void update_node_quota_default();
    // Returns value based on _node_quota_default
    inoutpair<quota_t> get_shard_quota_default() const;

    // void maybe_arm_balancer_timer();
    void notify_quota_balancer_node_period_change();
    std::chrono::milliseconds get_quota_balancer_node_period() const;
    void update_shard_quota_minimum();
    void quota_balancer();
    ss::future<> quota_balancer_step();
    ss::future<> quota_balancer_update();

    inoutpair<quota_t> get_deficiency() const noexcept {
      return to_each([] (const bottomless_token_bucket& b, const quota_t shard_quota_min) -> quota_t {
        if ( b.tokens() < 0 || b.quota() < shard_quota_min) {
          return 1;
        }
        return 0;
      }, _shard_quota, _shard_quota_minimum );
    }

    inoutpair<quota_t> get_quota() const noexcept {
        return to_each(&bottomless_token_bucket::quota, _shard_quota);
    }

    inoutpair<quota_t> get_surplus() const noexcept {
        return to_each(
          [](const bottomless_token_bucket& b, const quota_t& quota_min) {
              return std::max<quota_t>(
                b.get_current_rate() - quota_min, 0);
          }, _shard_quota, _shard_quota_minimum);
    }

    void adjust_quota(const inoutpair<quota_t>& delta) noexcept;

private:
    // immutable
    ss::sharded<class quota_manager>* _container{nullptr};

    // configuration
    config::binding<std::chrono::milliseconds> _max_kafka_throttle_delay;
    inoutpair<config::binding<std::optional<quota_t>>>
      _kafka_throughput_limit_node_bps;
    config::binding<std::chrono::milliseconds> _kafka_quota_balancer_window;
    config::binding<std::chrono::milliseconds>
      _kafka_quota_balancer_node_period;
    config::binding<double> _kafka_quota_balancer_min_shard_thoughput_ratio;
    config::binding<quota_t> _kafka_quota_balancer_min_shard_thoughput_bps;

    // operational, only used in the balancer shard
    ss::thread _balancer_thread;
    inoutpair<collect_dispense_amount_t> _shard_quotas_update;
    inoutpair<quota_t> _node_deficit;

    // operational, used on each shard
    inoutpair<std::optional<quota_t>> _node_quota_default;
    inoutpair<quota_t> _shard_quota_minimum;
    inoutpair<bottomless_token_bucket> _shard_quota;

    // service
    ss::abort_source _as;
    throughput_quotas_probe _probe;
};

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
class quota_manager : public ss::peering_sharded_service<quota_manager> {
public:
    using clock = ss::lowres_clock;

    struct throttle_delay {
        bool enforce{false};
        clock::duration duration{0};
        clock::duration enforce_duration() const {
            if (enforce) {
                return duration;
            } else {
                return clock::duration::zero();
            }
        }
    };

    quota_manager();
    quota_manager(const quota_manager&) = delete;
    quota_manager& operator=(const quota_manager&) = delete;
    quota_manager(quota_manager&&) = delete;
    quota_manager& operator=(quota_manager&&) = delete;
    ~quota_manager();

    ss::future<> stop();

    ss::future<> start();

    // record a new observation
    throttle_delay record_produce_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    // record a new observation
    void record_fetch_tp(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    throttle_delay throttle_fetch_tp(
      std::optional<std::string_view> client_id,
      clock::time_point now = clock::now());

    // Used to record new number of partitions mutations
    // Only for use with the quotas introduced by KIP-599, namely to track
    // partition creation and deletion events (create topics, delete topics &
    // create partitions)
    //
    // NOTE: This method will be invoked on shard 0, therefore ensure that it is
    // not called within a tight loop from another shard
    ss::future<std::chrono::milliseconds> record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now = clock::now());

    using shard_delays_t = snc_quota_manager::delays_t;
    
    /// Determine throttling required by shard level TP quotas.
    /// @param connection_throttle_until (in,out) until what time the client
    /// on this conection should throttle until. If it does not, this throttling
    /// will be enforced on the next call. In: value from the last call, out:
    /// value saved until the next call.
    shard_delays_t get_shard_delays(
      clock::time_point& connection_throttle_until,
      clock::time_point now) const {
        return _snc_qm.get_shard_delays(connection_throttle_until, now);
      }

    void record_request_tp(
      size_t request_size, clock::time_point now = clock::now()) noexcept {
        _snc_qm.record_request_tp(request_size,now);
      }

    void record_response_tp(
      size_t request_size, clock::time_point now = clock::now()) noexcept {
        _snc_qm.record_response_tp(request_size,now);
      }

    snc_quota_manager& snc_qm() noexcept { return _snc_qm; }
    const snc_quota_manager& snc_qm() const noexcept { return _snc_qm; }

private:
    std::chrono::milliseconds do_record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now);

    // throttle, return <previous delay, new delay>
    std::chrono::milliseconds throttle(
      std::optional<std::string_view> client_id,
      uint32_t target_rate,
      const clock::time_point& now,
      rate_tracker& rate_tracker);

    // Accounting for quota on per-client and per-client-group basis
    // last_seen: used for gc keepalive
    // delay: last calculated delay
    // tp_rate: throughput tracking
    // pm_rate: partition mutation quota tracking - only on home shard
    struct client_quota {
        clock::time_point last_seen;
        clock::duration delay;
        rate_tracker tp_produce_rate;
        void maybe_arm_balancer_timer();
        void notify_quota_balancer_node_period_change();
        void quota_balancer();
        ss::future<> quota_balancer_step();

        rate_tracker tp_fetch_rate;
        std::optional<token_bucket_rate_tracker> pm_rate;
    };
    using client_quotas_t = absl::flat_hash_map<ss::sstring, client_quota>;

private:
    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc(clock::duration full_window);

    client_quotas_t::iterator maybe_add_and_retrieve_quota(
      const std::optional<std::string_view>&, const clock::time_point&);
    int64_t get_client_target_produce_tp_rate(
      const std::optional<std::string_view>& quota_id);
    std::optional<int64_t> get_client_target_fetch_tp_rate(
      const std::optional<std::string_view>& quota_id);

private:
    config::binding<int16_t> _default_num_windows;
    config::binding<std::chrono::milliseconds> _default_window_width;

    config::binding<uint32_t> _default_target_produce_tp_rate;
    config::binding<std::optional<uint32_t>> _default_target_fetch_tp_rate;
    config::binding<std::optional<uint32_t>> _target_partition_mutation_quota;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_produce_tp_rate_per_client_group;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_fetch_tp_rate_per_client_group;

    client_quotas_t _client_quotas;

    ss::timer<> _gc_timer;
    clock::duration _gc_freq;
    config::binding<std::chrono::milliseconds> _max_delay;

    snc_quota_manager _snc_qm;
};

} // namespace kafka
