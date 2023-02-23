/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/property.h"
#include "seastarx.h"
#include "utils/bottomless_token_bucket.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <optional>

namespace kafka {

/// Represents a homogenous pair of values that correspond to
/// ingress and egress side of the same entity
template<class T>
struct ingress_egress_state {
    T in;
    T eg;
};

class snc_quotas_probe {
public:
    snc_quotas_probe(class snc_quota_manager& qm)
      : _qm(qm) {}
    snc_quotas_probe(const snc_quotas_probe&) = delete;
    snc_quotas_probe& operator=(const snc_quotas_probe&) = delete;
    snc_quotas_probe(snc_quotas_probe&&) = delete;
    snc_quotas_probe& operator=(snc_quotas_probe&&) = delete;
    ~snc_quotas_probe() noexcept = default;

    void rec_balancer_step() noexcept { ++_balancer_runs; }
    void rec_traffic_in(const size_t bytes) noexcept { _traffic_in += bytes; }

    void setup_metrics();

    uint64_t get_balancer_runs() const noexcept { return _balancer_runs; }

private:
    class snc_quota_manager& _qm;
    ss::metrics::metric_groups _metrics;
    uint64_t _balancer_runs = 0;
    size_t _traffic_in = 0;
};

/// Isolates \ref quota_manager functionality related to
/// shard/node/cluster (SNC) wide quotas and limits
class snc_quota_manager
  : public ss::peering_sharded_service<snc_quota_manager> {
public:
    using clock = ss::lowres_clock;
    using quota_t = bottomless_token_bucket::quota_t;

    snc_quota_manager();
    snc_quota_manager(const snc_quota_manager&) = delete;
    snc_quota_manager& operator=(const snc_quota_manager&) = delete;
    snc_quota_manager(snc_quota_manager&&) = delete;
    snc_quota_manager& operator=(snc_quota_manager&&) = delete;
    ~snc_quota_manager() noexcept = default;

    ss::future<> start();
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

    /// Record the request size when it has arrived from the transport.
    /// This should be done before calling \ref get_shard_delays because the
    /// recorded request size is used to calculate throttling parameters.
    void record_request_receive(
      size_t request_size, clock::time_point now = clock::now()) noexcept;

    /// Record the request size when the request data is about to be consumed.
    /// This data is used to represent throttled throughput.
    void record_request_intake(size_t request_size) noexcept;

    /// Record the response size for all purposes
    void record_response(
      size_t request_size, clock::time_point now = clock::now()) noexcept;

    /// Metrics probe object
    const snc_quotas_probe& get_snc_quotas_probe() const noexcept {
        return _probe;
    };

    /// Return current effective quota values
    ingress_egress_state<quota_t> get_quota() const noexcept;

private:
    // Returns value based on upstream values, not the _node_quota_default
    ingress_egress_state<std::optional<quota_t>>
    calc_node_quota_default() const;

    // Uses the func above to update _node_quota_default and dependencies
    void update_node_quota_default();

    ss::lowres_clock::duration get_quota_balancer_node_period() const;
    void update_shard_quota_minimum();

    /// Arm quota balancer timer at the distance of balancer period
    /// from the beginning of the last regular balancer run.
    /// \pre Current shard is the balancer shard
    void arm_balancer_timer();

    /// A step of regular quota balancer that reassigns parts of quota
    /// based on shards backpressure. Spawned by the balancer timer
    /// periodically. Runs on the balancer shard only
    ss::future<> quota_balancer_step();

    /// A step of balancer that applies any updates from configuration changes.
    /// Spawned by configration bindings watching changes of the properties.
    /// Runs on the balancer shard only.
    ss::future<> quota_balancer_update(
      ingress_egress_state<std::optional<quota_t>> old_node_quota_default,
      ingress_egress_state<std::optional<quota_t>> new_node_quota_default);

    /// Update time position of the buckets of the shard so that
    /// get_deficiency() and get_surplus() will return actual data
    void refill_buckets(const clock::time_point now) noexcept;

    /// If the current quota is sufficient for the shard, returns 0,
    /// otherwise returns a positive value
    ingress_egress_state<quota_t> get_deficiency() const noexcept;

    /// If the current quota is more than sufficient for the shard,
    /// returns how much it is more than sufficient as a positive value,
    /// otherwise returns 0.
    ingress_egress_state<quota_t> get_surplus() const noexcept;

    /// If the argument has a value, set the current shard quota to the value
    void maybe_set_quota(
      const ingress_egress_state<std::optional<quota_t>>&) noexcept;

    /// Increase or decrease the current shard quota by the argument
    void adjust_quota(const ingress_egress_state<quota_t>& delta) noexcept;

private:
    // configuration
    config::binding<std::chrono::milliseconds> _max_kafka_throttle_delay;
    ingress_egress_state<config::binding<std::optional<quota_t>>>
      _kafka_throughput_limit_node_bps;
    config::binding<std::chrono::milliseconds> _kafka_quota_balancer_window;
    config::binding<std::chrono::milliseconds>
      _kafka_quota_balancer_node_period;
    config::binding<double> _kafka_quota_balancer_min_shard_throughput_ratio;
    config::binding<quota_t> _kafka_quota_balancer_min_shard_throughput_bps;

    // operational, only used in the balancer shard
    ss::timer<ss::lowres_clock> _balancer_timer;
    ss::lowres_clock::time_point _balancer_timer_last_ran;
    ss::gate _balancer_gate;
    mutex _balancer_mx;
    ingress_egress_state<quota_t> _node_deficit{0, 0};

    // operational, used on each shard
    ingress_egress_state<std::optional<quota_t>> _node_quota_default;
    ingress_egress_state<quota_t> _shard_quota_minimum;
    ingress_egress_state<bottomless_token_bucket> _shard_quota;

    // service
    snc_quotas_probe _probe;
};

// Names exposed in this namespace are for unit test integration only
namespace detail {
snc_quota_manager::quota_t cap_to_ceiling(
  snc_quota_manager::quota_t& value, snc_quota_manager::quota_t limit);
void dispense_negative_deltas(
  std::vector<snc_quota_manager::quota_t>& schedule,
  snc_quota_manager::quota_t delta,
  std::vector<snc_quota_manager::quota_t> quotas);
void dispense_equally(
  std::vector<snc_quota_manager::quota_t>& target,
  snc_quota_manager::quota_t value);
} // namespace detail

} // namespace kafka
