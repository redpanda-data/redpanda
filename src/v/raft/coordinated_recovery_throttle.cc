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

#include "raft/coordinated_recovery_throttle.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "raft/logger.h"
#include "ssx/future-util.h"

#include <seastar/util/defer.hh>

#include <chrono>

namespace raft {

coordinated_recovery_throttle::token_bucket::token_bucket(
  size_t initial_capacity)
  : _sem(initial_capacity, "recovery_throttle")
  , _last_reset_capacity(initial_capacity) {}

ss::future<> coordinated_recovery_throttle::token_bucket::throttle(
  size_t size, ss::abort_source& as) {
    _waiting_bytes += size;
    auto decrement = ss::defer([this, size] { _waiting_bytes -= size; });
    co_await _sem.wait(as, size);
    _admitted_bytes_since_last_reset += size;
}

void coordinated_recovery_throttle::token_bucket::reset_capacity(
  size_t new_capacity) {
    auto current = _sem.current();
    if (current == new_capacity) {
        // nothing to do.
    } else if (current > new_capacity) {
        _sem.consume(current - new_capacity);
    } else {
        _sem.signal(new_capacity - current);
    }
    _admitted_bytes_since_last_reset = 0;
    _last_reset_capacity = new_capacity;
    vlog(
      raftlog.debug,
      "Throttler bucket capacity reset to: {}, waiting bytes: {}",
      new_capacity,
      _waiting_bytes);
}

coordinated_recovery_throttle::coordinated_recovery_throttle(
  config::binding<size_t> rate_binding, config::binding<bool> use_static)
  : _rate_binding(std::move(rate_binding))
  , _use_static_allocation(std::move(use_static))
  , _throttler(fair_rate_per_shard()) {
    if (ss::this_shard_id() == _coordinator_shard) {
        _coordinator.set_callback([this] {
            ssx::spawn_with_gate(_gate, [this] {
                return coordinate_tick().then(
                  [this] { arm_coordinator_timer(); });
            });
        });
    }
    setup_metrics();
}

void coordinated_recovery_throttle::setup_metrics() {
    if (!config::shard_local_cfg().disable_metrics()) {
        namespace sm = ss::metrics;
        _internal_metrics.add_group(
          prometheus_sanitize::metrics_name("raft:recovery"),
          {sm::make_gauge(
             "partition_movement_available_bandwidth",
             [this] { return _throttler.available(); },
             sm::description(
               "Bandwidth available for partition movement. bytes/sec")),

           sm::make_gauge(
             "partition_movement_assigned_bandwidth",
             [this] { return _throttler.last_reset_capacity(); },
             sm::description(
               "Bandwidth assigned for partition movement in last "
               "tick. bytes/sec"))});
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        namespace sm = ss::metrics;
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("raft:recovery"),
          {// note: deprecate partition_movement_available_bandwidth
           // in favor of partition_movement_consumed_bandwidth when
           // possible.
           sm::make_gauge(
             "partition_movement_available_bandwidth",
             [this] { return _throttler.available(); },
             sm::description(
               "Bandwidth available for partition movement. bytes/sec")),
           sm::make_gauge(
             "partition_movement_consumed_bandwidth",
             [this] {
                 return _throttler.last_reset_capacity()
                        - _throttler.available();
             },
             sm::description(
               "Bandwidth consumed for partition movement. bytes/sec"))});
    }
}

void coordinated_recovery_throttle::arm_coordinator_timer() {
    static constexpr clock_t::duration period = std::chrono::seconds{1};
    if (!_gate.is_closed()) {
        vassert(!_coordinator.armed(), "Invalid coordinator state: armed");
        _coordinator.arm(clock_t::now() + period);
    }
}

void coordinated_recovery_throttle::shutdown() { _throttler.shutdown(); }

ss::future<> coordinated_recovery_throttle::start() {
    vlog(raftlog.info, "Starting recovery throttle, rate: {}", _rate_binding());
    if (ss::this_shard_id() == _coordinator_shard) {
        arm_coordinator_timer();
    }
    co_return;
}

ss::future<> coordinated_recovery_throttle::stop() {
    vlog(raftlog.info, "Stopping recovery throttle");
    if (ss::this_shard_id() == _coordinator_shard) {
        co_await _gate.close();
    }
}

size_t coordinated_recovery_throttle::required_capacity() const {
    // Here we use admitted bytes in the last tick for temporal
    // locality. Since recovery happens as a series of sequential
    // RPCs, an admitted request in the current period highly likely
    // implies a next request soon. So we retain this periods worth
    // of capacity just in case.
    return _throttler.waiting_bytes() + _throttler.admitted_bytes();
}

ss::future<>
coordinated_recovery_throttle::reset_capacity_all_shards(size_t new_capacity) {
    co_await container().invoke_on_all(
      [new_capacity](coordinated_recovery_throttle& local) {
          local._throttler.reset_capacity(new_capacity);
      });
}

ss::future<> coordinated_recovery_throttle::reset_capacity_on_shard(
  ss::shard_id shard, size_t new_capacity) {
    co_await container().invoke_on(
      shard, [new_capacity](coordinated_recovery_throttle& local) {
          local._throttler.reset_capacity(new_capacity);
      });
}

ss::future<> coordinated_recovery_throttle::coordinate_tick() {
    try {
        co_await do_coordinate_tick();
    } catch (...) {
        vlog(
          raftlog.error,
          "Error coordinating recovery bandwidths: {}",
          std::current_exception());
    }
}

ss::future<> coordinated_recovery_throttle::do_coordinate_tick() {
    vassert(
      ss::this_shard_id() == _coordinator_shard,
      "Coordination on incorrect shard: {}",
      ss::this_shard_id());

    if (_gate.is_closed()) {
        co_return;
    }

    // Any updates to the rate are picked up in the next tick.
    auto total_rate = _rate_binding();
    auto fair_shard_rate = fair_rate_per_shard();

    if (unlikely(_use_static_allocation())) {
        co_return co_await reset_capacity_all_shards(fair_shard_rate);
    }

    // Step 1: Fetch the required capacity from all shards.
    const auto capacity_requirements = co_await container().map(
      [](coordinated_recovery_throttle& crt) {
          return crt.required_capacity();
      });

    // Step 2: Compute what unused bandwidth each shard has to offer.
    // Here we guarantee that each shard is assigned atleast fair_shard_rate
    // if needed. Anything beyond that is unused/deficit.
    size_t total_bandwidth_unused = 0;
    size_t total_bandwidth_deficit = 0;

    for (auto req : capacity_requirements) {
        if (req <= fair_shard_rate) {
            total_bandwidth_unused += (fair_shard_rate - req);
        } else {
            total_bandwidth_deficit += (req - fair_shard_rate);
        }
    }

    vlog(
      raftlog.trace,
      "Coordination tick: unused bandwidth: {}, deficit bandwidth: {}",
      total_bandwidth_unused,
      total_bandwidth_deficit);

    if (total_bandwidth_deficit == 0 || total_bandwidth_unused == 0) {
        // Most common case.
        // All shards operating well under fair capacity or every shard is
        // operating above the fair capacity and no extra bandwidth to offer.
        co_return co_await reset_capacity_all_shards(fair_shard_rate);
    }

    // Step 3: Here we try to distribute the unused bandwidth among shards with
    // deficit. in a weighted fashion proportional to the shard's share of
    // deficit.
    std::vector<ss::future<>> futures;
    futures.reserve(ss::smp::count);
    auto remaining_rate = total_rate;
    for (auto shard : boost::irange(ss::smp::count)) {
        auto req = capacity_requirements.at(shard);
        auto rate = fair_shard_rate;
        if (req <= fair_shard_rate) {
            rate = std::min(req, fair_shard_rate);
        } else {
            auto shard_deficit = req - fair_shard_rate;
            auto deficit_share = shard_deficit * 1.0L / total_bandwidth_deficit;
            rate = fair_shard_rate
                   + static_cast<size_t>(
                     deficit_share * total_bandwidth_unused);
        }
        remaining_rate -= rate;
        if (shard == ss::smp::count - 1) {
            // Assign any unused rate due to rounding of deficit share.
            rate = std::max(0UL, rate + remaining_rate);
        }
        futures.emplace_back(reset_capacity_on_shard(shard, rate));
    }
    co_await ss::when_all(futures.begin(), futures.end());
}

} // namespace raft
