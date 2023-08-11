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

#include "kafka/server/usage_aggregator.h"

using namespace std::chrono_literals;

namespace kafka {
static constexpr std::string_view period_key{"period"};
static constexpr std::string_view max_duration_key{"max_duration"};
static constexpr std::string_view buckets_key{"buckets"};

static bytes key_to_bytes(std::string_view sv) {
    bytes k;
    k.append(reinterpret_cast<const uint8_t*>(sv.begin()), sv.size());
    return k;
}

struct persisted_state {
    std::chrono::seconds configured_period;
    size_t configured_windows;
    fragmented_vector<usage_window> current_state;
};

static ss::future<>
persist_to_disk(storage::kvstore& kvstore, persisted_state s) {
    using kv_ks = storage::kvstore::key_space;

    co_await kvstore.put(
      kv_ks::usage,
      key_to_bytes(period_key),
      serde::to_iobuf(s.configured_period));
    co_await kvstore.put(
      kv_ks::usage,
      key_to_bytes(max_duration_key),
      serde::to_iobuf(s.configured_windows));
    co_await kvstore.put(
      kv_ks::usage,
      key_to_bytes(buckets_key),
      serde::to_iobuf(std::move(s.current_state)));
}

static std::optional<persisted_state>
restore_from_disk(storage::kvstore& kvstore) {
    using kv_ks = storage::kvstore::key_space;
    std::optional<iobuf> period, windows, data;
    try {
        period = kvstore.get(kv_ks::usage, key_to_bytes(period_key));
        windows = kvstore.get(kv_ks::usage, key_to_bytes(max_duration_key));
        data = kvstore.get(kv_ks::usage, key_to_bytes(buckets_key));
    } catch (const std::exception& ex) {
        vlog(
          klog.debug,
          "Encountered exception when retriving usage data from disk: {}",
          ex);
        return std::nullopt;
    }
    if (!period && !windows && !data) {
        /// Data didn't exist
        return std::nullopt;
    } else if (!period || !windows || !data) {
        vlog(
          klog.error,
          "Inconsistent usage_manager on disk state detected, failed to "
          "recover state");
        return std::nullopt;
    }
    return persisted_state{
      .configured_period = serde::from_iobuf<std::chrono::seconds>(
        std::move(*period)),
      .configured_windows = serde::from_iobuf<size_t>(std::move(*windows)),
      .current_state = serde::from_iobuf<fragmented_vector<usage_window>>(
        std::move(*data))};
}

static ss::future<> clear_persisted_state(storage::kvstore& kvstore) {
    using kv_ks = storage::kvstore::key_space;
    try {
        co_await kvstore.remove(kv_ks::usage, key_to_bytes(period_key));
        co_await kvstore.remove(kv_ks::usage, key_to_bytes(max_duration_key));
        co_await kvstore.remove(kv_ks::usage, key_to_bytes(buckets_key));
    } catch (const std::exception& ex) {
        vlog(klog.debug, "Ignoring exception from storage layer: {}", ex);
    }
}

usage usage::operator+(const usage& other) const {
    return usage{
      .bytes_sent = bytes_sent + other.bytes_sent,
      .bytes_received = bytes_received + other.bytes_received};
}

usage& usage::operator+=(const usage& other) {
    bytes_sent += other.bytes_sent;
    bytes_received += other.bytes_received;
    return *this;
}

template<typename clock_type, typename duration>
auto epoch_time_secs(std::chrono::time_point<clock_type, duration> now) {
    return std::chrono::duration_cast<std::chrono::seconds>(
             now.time_since_epoch())
      .count();
}

void usage_window::reset(uint64_t now) {
    begin = now;
    end = 0;
    u.bytes_sent = 0;
    u.bytes_received = 0;
    u.bytes_cloud_storage = std::nullopt;
}

template<typename clock_type>
usage_aggregator<clock_type>::usage_aggregator(
  storage::kvstore& kvstore,
  size_t usage_num_windows,
  std::chrono::seconds usage_window_width_interval,
  std::chrono::seconds usage_disk_persistance_interval)
  : _usage_num_windows(usage_num_windows)
  , _usage_window_width_interval(usage_window_width_interval)
  , _usage_disk_persistance_interval(usage_disk_persistance_interval)
  , _kvstore(kvstore) {
    vlog(
      klog.info,
      "Starting accounting fiber with settings, {{usage_num_windows: {} "
      "usage_window_width_interval: {} "
      "usage_disk_persistance_interval:{}}}",
      usage_num_windows,
      usage_window_width_interval,
      usage_disk_persistance_interval);
    _persist_disk_timer.set_callback([this] {
        ssx::background
          = ssx::spawn_with_gate_then(
              _bg_write_gate,
              [this] {
                  return persist_to_disk(
                    _kvstore,
                    persisted_state{
                      .configured_period = _usage_window_width_interval,
                      .configured_windows = _usage_num_windows,
                      .current_state = _buckets.copy()});
              })
              .then([this] {
                  if (!_gate.is_closed()) {
                      _persist_disk_timer.arm(_usage_disk_persistance_interval);
                  }
              })
              .handle_exception([this](std::exception_ptr eptr) {
                  vlog(
                    klog.debug,
                    "Encountered exception when persisting usage data to disk: "
                    "{} , retrying",
                    eptr);
                  if (!_gate.is_closed()) {
                      const auto retry = std::min(
                        _usage_disk_persistance_interval, 5s);
                      _persist_disk_timer.arm(clock_type::now() + retry);
                  }
              });
    });
    _close_window_timer.set_callback([this] {
        close_window();
        window_closed();
        rearm_window_timer();
    });
    /// TODO: This should be refactored when fragmented_vector::resize is
    /// implemented
    for (size_t i = 0; i < _usage_num_windows; ++i) {
        _buckets.push_back(usage_window{});
    }
}

template<typename clock_type>
void usage_aggregator<clock_type>::rearm_window_timer() {
    /// Calculate the next time the timer should fire, to adjust for skew ensure
    /// the timer always fires at the top of the interval, weather it be hour,
    /// minute, second, etc.
    static_assert(
      std::
        is_same_v<decltype(_usage_window_width_interval), std::chrono::seconds>,
      "Interval is assumed to be in units of seconds");
    const auto now = clock_type::now();
    /// This modulo trick only works because epoch time is hour aligned
    const auto delta = std::chrono::seconds(
      epoch_time_secs(now) % _usage_window_width_interval.count());
    const auto duration_until_next_close = _usage_window_width_interval - delta;
    vassert(
      duration_until_next_close >= 0s,
      "Error correctly detecting last window delta");
    _close_window_timer.arm(duration_until_next_close);
}

template<typename clock_type>
ss::future<> usage_aggregator<clock_type>::start() {
    /// In the event of a quick restart, reset_state() will set the
    /// _current_index to where it was before restart, however the total time
    /// until the next window must be accounted for. This is the duration for
    /// which redpanda was down.
    auto h = _gate.hold();
    auto state = restore_from_disk(_kvstore);
    bool successfully_restored = false;
    if (state) {
        if (
          state->configured_period != _usage_window_width_interval
          || state->configured_windows != _usage_num_windows) {
            vlog(
              klog.info,
              "Persisted usage state had been configured with different "
              "options, clearing state and restarting with current "
              "configuration options");
            co_await clear_persisted_state(_kvstore);
        } else {
            successfully_restored = true;
            reset_state(std::move(state->current_state));
        }
    }

    if (!successfully_restored) {
        const auto now = timestamp_t::now();
        const auto diff_secs = std::chrono::seconds(
          epoch_time_secs(now) % _usage_window_width_interval.count());
        _buckets[_current_window].reset(epoch_time_secs(now - diff_secs));
    }
    rearm_window_timer();
    _persist_disk_timer.arm(_usage_disk_persistance_interval);
}

template<typename clock_type>
ss::future<std::vector<usage_window>>
usage_aggregator<clock_type>::get_usage_stats() {
    /// Get the freshest data for the open bucket
    co_await grab_data(_current_window);

    std::vector<usage_window> stats;
    for (size_t i = 1; i < _buckets.size(); ++i) {
        const auto idx = (_current_window + i) % _usage_num_windows;
        if (!_buckets[idx].is_uninitialized()) {
            stats.push_back(_buckets[idx]);
        }
    }
    /// Open bucket last ensures ordering from oldest to newest
    stats.push_back(_buckets[_current_window]);
    /// std::reverse returns results in ordering from newest to oldest
    std::reverse(stats.begin(), stats.end());
    co_return stats;
}

template<typename clock_type>
ss::future<> usage_aggregator<clock_type>::stop() {
    /// Shutdown background work
    _close_window_timer.cancel();
    _persist_disk_timer.cancel();
    co_await _bg_write_gate.close();
    try {
        /// Fetch freshest window, if possible
        co_await grab_data(_current_window);
        /// Prevent further calls to grab_data to succeed
        _m.broken();
        /// Write freshest buckets to disk
        co_await persist_to_disk(
          _kvstore,
          persisted_state{
            .configured_period = _usage_window_width_interval,
            .configured_windows = _usage_num_windows,
            .current_state = _buckets.copy()});
    } catch (const std::exception& ex) {
        vlog(
          klog.debug,
          "Encountered exception when persisting usage data to disk: {}",
          ex);
    }
    co_await _gate.close();
}

/// Method to write response to correct bucket, the index to write should be
/// \ref index, however if enough time has passed that window has been
/// overwritten, therefore the data can be omitted
template<typename clock_type>
bool usage_aggregator<clock_type>::is_bucket_stale(
  size_t idx, uint64_t open_ts) const {
    /// Check is simple, open_ts should be what it was when the window
    /// opened, if it isn't, then the window is stale and was re-used,
    /// data was overwritten.
    return _buckets[idx].begin != open_ts;
}

/// Only fiber of execution responsible for grabbing the results
/// from all cores, resetting their respective counters and caching
/// the result in memory
template<typename clock_type>
ss::future<> usage_aggregator<clock_type>::grab_data(size_t idx) {
    auto gh = _gate.hold();
    try {
        auto units = co_await _m.get_units();
        const auto ts = _buckets[idx].begin;
        /// Grab the data, across this scheduling point we cannot assume things
        /// like _current_window have the same value as before this call, that
        /// is why _current_window is locally cached
        const auto usage_data = co_await close_current_window();
        if (!is_bucket_stale(idx, ts)) {
            _buckets[idx].u += usage_data;
            _buckets[idx].u.bytes_cloud_storage
              = usage_data.bytes_cloud_storage;
        }
    } catch (const std::exception& e) {
        vlog(
          klog.info, "encountered error when attempting to fetch data: {}", e);
    }
}

template<typename clock_type>
void usage_aggregator<clock_type>::close_window() {
    /// The timer should fire at the top of the interval, to account for
    /// rounding issues when converting to epoch time in seconds, we will force
    /// round to nearest interval as long as the difference is within a small
    /// degree of tolerance
    const uint64_t interval = _usage_window_width_interval.count();
    const auto now = detail::round_to_interval(
      _usage_window_width_interval, timestamp_t::now());
    const auto now_ts = epoch_time_secs(now);
    auto& cur = _buckets[_current_window];
    cur.end = now_ts;
    if ((cur.end - cur.begin) != interval) {
        const auto err_str = fmt::format(
          "Observed a bucket (with index {}) that begin ts {} and end "
          "ts of {}, this means a timer had fired earlier or later "
          "then expected, then rounded to the equivalent value of the begin "
          "timestamp. This current window will be dropped and a new one opened",
          _current_window,
          cur.begin,
          cur.end);
        /// Logging at error level for wider intervals because this is more
        /// likely to be an issue for applications with second granularity
        /// intervals which in reality would only be the ducktape tests, logging
        /// at error would cause them to fail.
        if (_usage_window_width_interval > 2min) {
            vlog(klog.error, "{}", err_str);
        } else {
            vlog(klog.info, "{}", err_str);
        }
        cur.reset(now_ts);
    } else {
        const auto w = _current_window;
        _current_window = (_current_window + 1) % _buckets.size();
        _buckets[_current_window].reset(now_ts);
        ssx::background = ssx::spawn_with_gate_then(
          _gate, [this, w]() { return grab_data(w); });
    }
}

template<typename clock_type>
void usage_aggregator<clock_type>::reset_state(
  fragmented_vector<usage_window> buckets) {
    /// called after restart to determine which bucket is the 'current' bucket
    _current_window = 0;
    if (!buckets.empty()) {
        std::optional<size_t> open_index;
        for (size_t i = 0; i < buckets.size(); ++i) {
            /// There will always be exactly 1 open_window in the result set
            if (buckets[i].is_open()) {
                vassert(!open_index, "Data serialization was incorrect");
                open_index = i;
                break;
            }
        }
        vassert(open_index, "Data serialization was incorrect");
        _current_window = *open_index;
        /// Optimization to begin picking up if wall time is within
        /// window interval
        const auto begin = std::chrono::seconds(buckets[*open_index].begin);
        const auto now_ts = timestamp_t::now();
        const auto now = std::chrono::seconds(epoch_time_secs(now_ts));
        const auto delta = now - begin;
        if (delta >= _usage_window_width_interval) {
            /// Close window and open a new one
            const auto begin_ts = typename timestamp_t::time_point(begin);
            buckets[*open_index].end = epoch_time_secs(
              begin_ts + _usage_window_width_interval);
            _current_window = (*open_index + 1) % buckets.size();
            const auto diff_secs = std::chrono::seconds(
              epoch_time_secs(now_ts) % _usage_window_width_interval.count());
            buckets[_current_window].reset(epoch_time_secs(now_ts - diff_secs));
        }
    }
    _buckets = std::move(buckets);
}

template class usage_aggregator<ss::lowres_clock>;
template class usage_aggregator<ss::manual_clock>;

} // namespace kafka
