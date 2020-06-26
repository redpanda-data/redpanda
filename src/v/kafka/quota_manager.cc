#include "kafka/quota_manager.h"

#include "config/configuration.h"
#include "kafka/logger.h"
#include "vlog.h"

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

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
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
      ss::sstring(cid),
      quota{
        now,
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
    if (delay_ms > (uint64_t)_max_delay.count()) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, Estimated "
          "backpressure delay of {}ms. Limiting to {}ms backpressure delay",
          rate,
          cid,
          delay_ms,
          _max_delay.count());
        delay_ms = _max_delay.count();
    }

    auto prev = it->second.delay;
    it->second.delay = std::chrono::milliseconds(delay_ms);

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
