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

#include <boost/range/irange.hpp>

#include <chrono>
#include <cstdio>
#include <iterator>
#include <limits>

namespace raft {

coordinated_recovery_throttle::token_bucket::token_bucket(
  size_t initial_capacity)
  : _available_units(initial_capacity)
  , _last_reset_capacity(initial_capacity) {
    _cv.signal();
}

ss::future<> coordinated_recovery_throttle::token_bucket::throttle(
  size_t size, ss::abort_source& as) {
    _waiting_bytes += size;
    auto decrement = ss::defer([this, size] { _waiting_bytes -= size; });
    auto lock = co_await _mutex.get_units(as);
    while (true) {
        co_await _cv.wait(as);
        bool can_consume = _available_units >= static_cast<ssize_t>(size);
        // We are first in the queue in this tick, but maybe our request is
        // too big. Allow borrowing from future periods.
        bool allowed_to_borrow = _admitted_bytes_since_last_reset == 0
                                 && _available_units > 0;
        vlog(
          raftlog.trace,
          "throttler: size={}, _available_units={}, "
          "_admitted_bytes_since_last_reset={}, can_consume={}, "
          "allowed_to_borrow={}",
          size,
          _available_units,
          _admitted_bytes_since_last_reset,
          can_consume,
          allowed_to_borrow);
        if (can_consume || allowed_to_borrow) {
            _available_units -= size;
            _admitted_bytes_since_last_reset += size;
            _cv.signal();
            vlog(
              raftlog.trace,
              "throttler: admitted size={}, remaining capacity={}",
              size,
              _available_units);
            co_return;
        }
    }
}

void coordinated_recovery_throttle::token_bucket::renew_capacity(
  ssize_t added_capacity) {
    _available_units += added_capacity;
    _last_reset_capacity = _available_units;
    _admitted_bytes_since_last_reset = 0;
    vlog(
      raftlog.debug,
      "Resetting throttler bucket capacity to: {}, waiting bytes: {}",
      _last_reset_capacity,
      _waiting_bytes);
    if (_available_units > 0) {
        _cv.signal();
    }
}

coordinated_recovery_throttle::coordinated_recovery_throttle(
  config::binding<size_t> rate_binding, config::binding<bool> use_static)
  : _rate_binding(std::move(rate_binding))
  , _use_static_allocation(std::move(use_static))
  , _throttler(_rate_binding() / ss::smp::count) {
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
    vlog(raftlog.info, "Stopped recovery throttle");
}

coordinated_recovery_throttle::shard_capacity_request
coordinated_recovery_throttle::required_capacity() const {
    return {
      .remaining_units = _throttler.available(),
      // Here we use admitted bytes in the last tick for temporal
      // locality. Since recovery happens as a series of sequential
      // RPCs, an admitted request in the current period highly likely
      // implies a next request soon. So we retain this periods worth
      // of capacity just in case.
      .desired_additional_capacity = _throttler.waiting_bytes()
                                     + _throttler.admitted_bytes()};
}

ss::future<> coordinated_recovery_throttle::renew_capacity_all_shards(
  ssize_t added_capacity) {
    co_await container().invoke_on_all(
      [added_capacity](coordinated_recovery_throttle& local) {
          local._throttler.renew_capacity(added_capacity);
      });
}

ss::future<> coordinated_recovery_throttle::renew_capacity_on_shard(
  ss::shard_id shard, ssize_t added_capacity) {
    co_await container().invoke_on(
      shard, [added_capacity](coordinated_recovery_throttle& local) {
          local._throttler.renew_capacity(added_capacity);
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

    // Any updates to the rate are picked up here in the beginning of the tick.
    // Clamp up for each shard to receive at least one unit.
    // Clamp down to avoid overflow.
    // Retain zero as it is to block all recovery (used in tests).
    ssize_t total_rate = _rate_binding()
                           ? std::clamp(
                               _rate_binding(),
                               size_t{ss::smp::count},
                               std::numeric_limits<size_t>::max() / 4)
                           : 0;

    // Fetch the requirements from all shards.
    const auto capacity_requirements = co_await container().map(
      std::mem_fn(&coordinated_recovery_throttle::required_capacity));

    // Remaining units if positive, overused if negative
    ssize_t total_remaining_units = 0;
    for (const auto& req : capacity_requirements) {
        total_remaining_units += req.remaining_units;
        vlog(
          raftlog.trace,
          "Shard capacity request: remaining_units={}, "
          "desired_additional_capacity={}",
          req.remaining_units,
          req.desired_additional_capacity);
    }

    ssize_t unused_units = std::max(ssize_t{0}, total_remaining_units);
    // Do not carry over to the next tick. What is unused is lost.
    // Can be negative if config changed to a lower rate
    ssize_t to_add = total_rate - unused_units;

    if (unlikely(_use_static_allocation())) {
        // Don't take individual shard requests into account, distribute equally
        auto fair_shard_rate = to_add / ss::smp::count;
        co_return co_await renew_capacity_all_shards(fair_shard_rate);
    }

    // Surplus after we satisfy hard requirements (that is, bring all shard
    // rates to 0 assuming they stay as they are while we're calculating).
    // Negative means net overuse.
    ssize_t avg_surplus = (to_add + total_remaining_units) / ss::smp::count;
    vlog(
      raftlog.trace,
      "Coordinated recovery throttle tick: total_rate={}, "
      "total_remaining_units={}, unused_units={}, to_add={}, "
      "avg_surplus={}",
      total_rate,
      total_remaining_units,
      unused_units,
      to_add,
      avg_surplus);

    // Process "soft" requests. Under-/overrequested means requested less/more
    // than avg_surplus.
    size_t total_underrequested = 0;
    size_t total_overrequested = 0;

    if (avg_surplus <= 0) {
        // Do not bother redistributing according to "soft" requests, we cannot
        // even repay the debts to make allowed rate non-negative on all shards.
    } else {
        auto get_overrequest =
          [avg_surplus](const shard_capacity_request& req) {
              return static_cast<ssize_t>(req.desired_additional_capacity)
                     - avg_surplus;
          };
        for (const auto& req : capacity_requirements) {
            auto overrequest = get_overrequest(req);
            if (overrequest > 0) {
                total_overrequested += overrequest;
            } else {
                total_underrequested += -overrequest;
            }
        }
        if (total_overrequested != 0 && total_underrequested != 0) {
            // Here we try to distribute the under-requested bandwidth among
            // shards with over-requested bandwidth. We do it proportionally to
            // the shard's share of overrequest.
            co_return co_await renew_capacity_on_shards(
              capacity_requirements,
              [avg_surplus,
               total_underrequested,
               total_overrequested,
               &get_overrequest](const shard_capacity_request& req) {
                  auto rate = -req.remaining_units + avg_surplus;
                  auto overrequest = get_overrequest(req);
                  if (overrequest <= 0) {
                      // underrequested: grant what they asked for
                      rate += overrequest;
                  } else {
                      // overrequested: get a share of regained from
                      // underrequested, proportional to overrequest
                      double share = 1.0 * overrequest / total_overrequested;
                      rate += static_cast<ssize_t>(
                        total_underrequested * share);
                  }
                  return rate;
              });
        }
    }

    // Most common case.
    // All shards operating well under fair capacity or every shard is
    // operating above the fair capacity and no extra bandwidth to offer.
    co_await renew_capacity_on_shards(
      capacity_requirements, [avg_surplus](const shard_capacity_request& req) {
          return -req.remaining_units + avg_surplus;
      });
}

template<typename Func>
ss::future<> coordinated_recovery_throttle::renew_capacity_on_shards(
  const std::vector<shard_capacity_request>& requests,
  Func&& added_capacity_calculator) {
    auto futures = requests
                   | std::views::transform(
                     [this, &added_capacity_calculator, shard(ss::shard_id{0})](
                       const shard_capacity_request& req) mutable {
                         return renew_capacity_on_shard(
                           shard++, added_capacity_calculator(req));
                     });
    co_await ss::when_all(futures.begin(), futures.end());
}
} // namespace raft
