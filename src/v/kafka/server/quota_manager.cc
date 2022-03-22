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
#include "vlog.h"

#include <fmt/chrono.h>

#include <chrono>

static std::string_view extract_cid(std::optional<std::string_view> client_id) {
    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    return client_id ? *client_id : "";
}

namespace kafka {
using clock = quota_manager::clock;
using throttle_delay = quota_manager::throttle_delay;

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    return ss::make_ready_future<>();
}

ss::future<> quota_manager::start() {
    _gc_timer.arm_periodic(_gc_freq);
    return ss::make_ready_future<>();
}

quota_manager::underlying_t::iterator
quota_manager::grab_quota(std::string_view cid, const clock::time_point& now) {
    // find or create the throughput tracker for this client
    //
    // c++20: heterogeneous lookup for unordered_map can avoid creation of
    // an sstring here but std::unordered_map::find isn't using an
    // equal_to<> overload. this is a general issue we'll be looking at. for
    // now, these client-name strings are small. This will be solved in
    // c++20 via Hash::transparent_key_equal.
    auto [it, inserted] = _quotas.try_emplace(
      ss::sstring(cid),
      quota{
        now,
        clock::duration(0),
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        {static_cast<size_t>(_default_num_windows()),
         _default_window_width()}});

    // bump to prevent gc
    if (!inserted) {
        it->second.last_seen = now;
    }
    return it;
}

void quota_manager::record_time_quota(
  std::string_view client_id, clock::time_point start, clock::time_point now) {
    auto it = grab_quota(client_id, now);
    auto duration = now - start;
    it->second.time_rate.record(
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count(),
      now);
}

ss::lw_shared_ptr<ss::noncopyable_function<void()>>
quota_manager::time_quota_recorder(std::optional<std::string_view> client_id) {
    return ss::make_lw_shared<ss::noncopyable_function<void()>>(
      [this,
       recorded = false,
       cid = ss::sstring(extract_cid(client_id)),
       start = clock::now()]() mutable noexcept {
          /// The \ref recorded variable prevents misuse of the returned
          /// functor. The desired behavior is that for each callable returned,
          /// it either is called 1 or 0 times.
          if (!recorded) {
              record_time_quota(cid, start, clock::now());
          }
          recorded = true;
      });
}

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto cid = extract_cid(client_id);
    auto it = grab_quota(cid, now);

    auto bandwidth_rate = it->second.tp_rate.record_and_measure(bytes, now);

    std::chrono::milliseconds bandwidth_delay_ms(0);
    if (bandwidth_rate > _target_tp_rate()) {
        auto diff = bandwidth_rate - _target_tp_rate();
        double delay = (diff / _target_tp_rate())
                       * static_cast<double>(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                           it->second.tp_rate.window_size())
                           .count());
        bandwidth_delay_ms = std::chrono::milliseconds(
          static_cast<uint64_t>(delay));
    }

    auto request_rate = std::chrono::milliseconds(
      static_cast<uint64_t>(it->second.time_rate.measure(now)));

    std::chrono::milliseconds request_delay_ms(0);
    if (request_rate > _default_window_width()) {
        request_delay_ms = request_rate - _default_window_width();
    }

    /// Take the larger of the two delays
    std::chrono::milliseconds delay_ms = std::max(
      bandwidth_delay_ms, request_delay_ms);

    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          bandwidth_rate,
          cid,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }

    auto prev = it->second.delay;
    it->second.delay = delay_ms;

    throttle_delay res{};
    res.first_violation = prev.count() == 0;
    res.duration = it->second.delay;
    return res;
}
// erase inactive tracked quotas. windows are considered inactive if they
// have not received any updates in ten window's worth of time.
void quota_manager::gc(clock::duration full_window) {
    auto now = clock::now();
    auto expire_age = full_window * 10;
    // c++20: replace with std::erase_if
    absl::erase_if(
      _quotas, [now, expire_age](const std::pair<ss::sstring, quota>& q) {
          return (now - q.second.last_seen) > expire_age;
      });
}

} // namespace kafka
