#pragma once
#include "config/configuration.h"
#include "resource_mgmt/rate.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace kafka {

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
class quota_manager {
public:
    using clock = seastar::lowres_clock;

    struct throttle_delay {
        bool first_violation;
        clock::duration duration;
    };

    quota_manager()
      : _default_num_windows(config::shard_local_cfg().default_num_windows())
      , _default_window_width(config::shard_local_cfg().default_window_sec())
      , _target_tp_rate(config::shard_local_cfg().target_quota_byte_rate())
      , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec()) {
        auto full_window = _default_num_windows * _default_window_width;
        _gc_timer.set_callback([this, full_window] { gc(full_window); });
    }

    ~quota_manager() { _gc_timer.cancel(); }

    seastar::future<> stop() {
        _gc_timer.cancel();
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _gc_timer.arm_periodic(_gc_freq);
        return seastar::make_ready_future<>();
    }

    // record a new observation and return <previous delay, new delay>
    throttle_delay record_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now()) {
        // requests without a client id are grouped into an anonymous group that
        // shares a default quota. the anonymous group is keyed on empty string.
        auto cid = client_id ? *client_id : "";

        // find or create the throughput tracker for this client
        //
        // c++20: heterogeneous lookup for unordered_map can avoid creation of
        // an sstring here but std::unordered_map::find isn't using an
        // equal_to<> overload. this is a general issue we'll be looking at. for
        // now, these client-name strings are small. This will be solved in
        // c++20 via Hash::transparent_key_equal.
        auto [it, inserted] = _quotas.try_emplace(
          seastar::sstring(cid),
          quota{now,
                clock::duration(0),
                {_default_num_windows, _default_window_width}});

        // bump to prevent gc
        if (!inserted) {
            it->second.last_seen = now;
        }

        auto rate = it->second.tp_rate.record_and_measure(bytes, now);

        uint64_t delay_ms = 0;
        if (rate > _target_tp_rate) {
            auto diff = rate - _target_tp_rate;
            double delay
              = (diff / _target_tp_rate)
                * (double)std::chrono::duration_cast<std::chrono::milliseconds>(
                    it->second.tp_rate.window_size())
                    .count();
            delay_ms = static_cast<uint64_t>(delay);
        }

        auto prev = it->second.delay;
        it->second.delay = std::chrono::milliseconds(delay_ms);

        throttle_delay res{};
        res.first_violation = prev.count() == 0;
        res.duration = it->second.delay;
        return res;
    }

private:
    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc(clock::duration full_window) {
        auto now = clock::now();
        auto expire_age = full_window * 10;
        // c++20: replace with std::erase_if
        for (auto it = _quotas.begin(); it != _quotas.end();) {
            if ((now - it->second.last_seen) > expire_age) {
                it = _quotas.erase(it);
            } else {
                it++;
            }
        }
    }

private:
    // last_seen: used for gc keepalive
    // delay: last calculated delay
    // tp_rate: throughput tracking
    struct quota {
        clock::time_point last_seen;
        clock::duration delay;
        rate_tracker tp_rate;
    };

    const std::size_t _default_num_windows;
    const clock::duration _default_window_width;

    const uint32_t _target_tp_rate;
    std::unordered_map<seastar::sstring, quota> _quotas;

    seastar::timer<> _gc_timer;
    const clock::duration _gc_freq;
};

} // namespace kafka
