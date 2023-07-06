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

#include "kafka/server/logger.h"
#include "vlog.h"

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

template<typename time_point>
auto epoch_time_secs(time_point now) {
    return std::chrono::duration_cast<std::chrono::seconds>(
             now.time_since_epoch())
      .count();
}

static ss::lowres_system_clock::time_point round_to_interval(
  std::chrono::seconds usage_window_width_interval,
  ss::lowres_system_clock::time_point t) {
    /// Downstream systems are particularly sensitive to minor issues with
    /// timestamps not triggering on the configured interval (hours, minutes,
    /// seconds, etc), this method rounds t to the nearest interval and logs an
    /// error if this cannot be done within some threshold.
    const auto interval = usage_window_width_interval;
    const auto err_threshold = interval < 2min ? interval : 2min;
    const auto cur_interval_start = t - (t.time_since_epoch() % interval);
    const auto next_interval_start = cur_interval_start + interval;
    if (t - cur_interval_start <= err_threshold) {
        return {cur_interval_start};
    } else if (next_interval_start - t <= err_threshold) {
        return {next_interval_start};
    }
    vlog(
      klog.error,
      "usage has detected a timestamp '{}' that exceeds the preconfigured "
      "threshold of 2min meaning a clock has fired later or earlier then "
      "expected, this is unexpected behavior and should be investigated.",
      t.time_since_epoch().count());
    return t;
}

void usage_window::reset(ss::lowres_system_clock::time_point now) {
    begin = epoch_time_secs(now);
    end = 0;
    u.bytes_sent = 0;
    u.bytes_received = 0;
    u.bytes_cloud_storage = std::nullopt;
}

void usage_window::reset_to_nearest_interval(
  std::chrono::seconds usage_window_width_interval,
  ss::lowres_system_clock::time_point tp) {
    /// Rounds now back to nearest hour, minute, second or more generally window
    /// width, this way all windows are always aligned to eachother,
    /// clusterwide.
    const auto diff_secs = std::chrono::seconds(
      epoch_time_secs(tp) % usage_window_width_interval.count());
    reset(tp - diff_secs);
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
    _timer.set_callback([this] {
        ssx::background = ssx::spawn_with_gate_then(_gate, [this]() {
                              return close_window();
                          }).finally([this] {
            if (!_gate.is_closed()) {
                rearm_window_timer();
            }
        });
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
    _timer.arm(duration_until_next_close);
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
        _buckets[_current_window].reset_to_nearest_interval(
          _usage_window_width_interval, ss::lowres_system_clock::now());
    }
    rearm_window_timer();
    _persist_disk_timer.arm(_usage_disk_persistance_interval);
}

template<typename clock_type>
std::vector<usage_window> usage_aggregator<clock_type>::get_usage_stats() {
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
    return stats;
}

template<typename clock_type>
ss::future<> usage_aggregator<clock_type>::stop() {
    _timer.cancel();
    _persist_disk_timer.cancel();
    co_await _bg_write_gate.close();
    try {
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
  size_t idx, uint64_t close_ts) const {
    /// Check is simple, close_ts should be what it was when the window
    /// closed, if it isn't, then the window is stale and was re-used,
    /// data was overwritten.
    return _buckets[idx].end != close_ts;
}

template<typename clock_type>
void usage_aggregator<clock_type>::close_window() {
    /// The timer should fire at the top of the interval, to account for
    /// rounding issues when converting to epoch time in seconds, we will force
    /// round to nearest interval as long as the difference is within a small
    /// degree of tolerance
    const auto now = round_to_interval(
      _usage_window_width_interval, ss::lowres_system_clock::now());
    const auto now_ts = epoch_time_secs(now);
    const auto before_close_idx = _current_window;
    _buckets[before_close_idx].end = now_ts;
    _current_window = (_current_window + 1) % _buckets.size();
    _buckets[_current_window].reset(now);
    /// The timer must progress so subsequent windows may be closed, async work
    /// may hold that up for a significant amount of time, therefore async work
    /// will be dispatched in the background and the result set written to the
    /// correct bucket when it is eventually computed
    ssx::spawn_with_gate(_gate, [this, before_close_idx, close_ts = now_ts] {
        return close_current_window()
          .then([this, before_close_idx, close_ts](usage next) {
              if (!is_bucket_stale(before_close_idx, close_ts)) {
                  _buckets[before_close_idx].u = next;
              }
          })
          .handle_exception([](std::exception_ptr eptr) {
              vlog(
                klog.debug,
                "usage_manager async job exception encountered: {}",
                eptr);
          });
    });
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
        const auto now_ts = ss::lowres_system_clock::now();
        const auto now = std::chrono::seconds(epoch_time_secs(now_ts));
        const auto delta = now - begin;
        if (delta >= _usage_window_width_interval) {
            /// Close window and open a new one
            const auto begin_ts = ss::lowres_system_clock::time_point(begin);
            buckets[*open_index].end = epoch_time_secs(
              begin_ts + _usage_window_width_interval);
            _current_window = (*open_index + 1) % buckets.size();
            buckets[_current_window].reset_to_nearest_interval(
              _usage_window_width_interval, now_ts);
        }
    }
    _buckets = std::move(buckets);
}

template class usage_aggregator<ss::lowres_clock>;
template class usage_aggregator<ss::manual_clock>;

} // namespace kafka
