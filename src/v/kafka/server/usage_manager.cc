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

#include "kafka/server/usage_manager.h"

#include "cluster/health_monitor_frontend.h"
#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "ssx/future-util.h"
#include "storage/api.h"
#include "vlog.h"

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

static auto epoch_time_secs(
  ss::lowres_system_clock::time_point now = ss::lowres_system_clock::now()) {
    return std::chrono::duration_cast<std::chrono::seconds>(
             now.time_since_epoch())
      .count();
}

void usage_window::reset(ss::lowres_system_clock::time_point now) {
    begin = epoch_time_secs(now);
    end = 0;
    u.bytes_sent = 0;
    u.bytes_received = 0;
    u.bytes_cloud_storage = 0;
}

usage usage::operator+(const usage& other) const {
    return usage{
      .bytes_sent = bytes_sent + other.bytes_sent,
      .bytes_received = bytes_received + other.bytes_received};
}

usage_manager::accounting_fiber::accounting_fiber(
  ss::sharded<usage_manager>& um,
  ss::sharded<cluster::health_monitor_frontend>& health_monitor,
  ss::sharded<storage::api>& storage,
  size_t usage_num_windows,
  std::chrono::seconds usage_window_width_interval,
  std::chrono::seconds usage_disk_persistance_interval)
  : _usage_num_windows(usage_num_windows)
  , _usage_window_width_interval(usage_window_width_interval)
  , _usage_disk_persistance_interval(usage_disk_persistance_interval)
  , _health_monitor(health_monitor.local())
  , _kvstore(storage.local().kvs())
  , _um(um) {
    vlog(
      klog.info,
      "Starting accounting fiber with settings, {{usage_num_windows: {} "
      "usage_window_width_interval: {} "
      "usage_disk_persistance_interval:{}}}",
      usage_num_windows,
      usage_window_width_interval,
      usage_disk_persistance_interval);
    /// TODO: This should be refactored when fragmented_vector::resize is
    /// implemented
    for (size_t i = 0; i < _usage_num_windows; ++i) {
        _buckets.push_back(usage_window{});
    }
    _buckets[_current_window].reset(ss::lowres_system_clock::now());
}

ss::future<> usage_manager::accounting_fiber::start() {
    /// In the event of a quick restart, reset_state() will set the
    /// _current_index to where it was before restart, however the total time
    /// until the next window must be accounted for. This is the duration for
    /// which redpanda was down.
    auto h = _gate.hold();
    auto last_window_delta = std::chrono::seconds(0);
    auto state = restore_from_disk(_kvstore);
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
            last_window_delta = reset_state(std::move(state->current_state));
        }
    }
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
                      _persist_disk_timer.arm(
                        ss::lowres_clock::now()
                        + _usage_disk_persistance_interval);
                  }
              })
              .handle_exception([this](std::exception_ptr eptr) {
                  using namespace std::chrono_literals;
                  vlog(
                    klog.debug,
                    "Encountered exception when persisting usage data to disk: "
                    "{} , retrying",
                    eptr);
                  if (!_gate.is_closed()) {
                      const auto retry = std::min(
                        _usage_disk_persistance_interval, 5s);
                      _persist_disk_timer.arm(ss::lowres_clock::now() + retry);
                  }
              });
    });
    _timer.set_callback([this] {
        ssx::background = ssx::spawn_with_gate_then(_gate, [this]() {
                              return close_window();
                          }).finally([this] {
            if (!_gate.is_closed()) {
                _timer.arm(
                  ss::lowres_clock::now() + _usage_window_width_interval);
            }
        });
    });
    const auto now = ss::lowres_clock::now();
    vassert(
      last_window_delta <= _usage_window_width_interval,
      "Error correctly detecting last window delta");
    _timer.arm((now + _usage_window_width_interval) - last_window_delta);
    _persist_disk_timer.arm(now + _usage_disk_persistance_interval);
}

std::vector<usage_window>
usage_manager::accounting_fiber::get_usage_stats() const {
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

ss::future<> usage_manager::accounting_fiber::stop() {
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

void usage_manager::accounting_fiber::close_window() {
    const auto now = ss::lowres_system_clock::now();
    const auto now_ts = epoch_time_secs(now);
    const auto before_close_idx = _current_window;
    _buckets[before_close_idx].end = now_ts;
    _current_window = (_current_window + 1) % _buckets.size();
    _buckets[_current_window].reset(now);
    /// The timer must progress so subsequent windows may be closed, async work
    /// may hold that up for a significant amount of time, therefore async work
    /// will be dispatched in the background and the result set written to the
    /// correct bucket when it is eventually computed
    ssx::spawn_with_gate(_gate, [this, before_close_idx, now_ts] {
        return async_data_fetch(before_close_idx, now_ts)
          .handle_exception([](std::exception_ptr eptr) {
              vlog(
                klog.debug,
                "usage_manager async job exception encountered: {}",
                eptr);
          });
    });
}

ss::future<> usage_manager::accounting_fiber::async_data_fetch(
  size_t index, uint64_t close_ts) {
    /// Method to write response to correct bucket, the index to write should be
    /// \ref index, however if enough time has passed that window has been
    /// overwritten, therefore the data can be omitted
    const auto is_bucket_stale = [this, index, close_ts]() {
        /// Check is simple, close_ts should be what it was when the window
        /// closed, if it isn't, then the window is stale and was re-used, data
        /// was overwritten.
        return _buckets[index].end != close_ts;
    };

    /// Collect all kafka ingress/egress stats across all cores
    usage kafka_stats = co_await _um.map_reduce0(
      [](usage_manager& um) { return um.sample(); },
      usage{},
      [](const usage& acc, const usage& x) { return acc + x; });
    if (!is_bucket_stale()) {
        _buckets[index].u = kafka_stats;
    }

    /// Grab cloud storage stats via health monitor
    const auto expiry = std::min<std::chrono::seconds>(
      (_usage_window_width_interval * _usage_num_windows),
      std::chrono::seconds(10));
    co_await _health_monitor.maybe_refresh_cloud_health_stats();
    auto health_overview = co_await _health_monitor.get_cluster_health_overview(
      ss::lowres_clock::now() + expiry);
    if (!is_bucket_stale()) {
        _buckets[index].u.bytes_cloud_storage
          = health_overview.bytes_in_cloud_storage;
    }
}

std::chrono::seconds usage_manager::accounting_fiber::reset_state(
  fragmented_vector<usage_window> buckets) {
    /// called after restart to determine which bucket is the 'current' bucket
    auto last_window_delta = std::chrono::seconds(0);
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
            buckets[*open_index].end = epoch_time_secs(now_ts);
            _current_window = (*open_index + 1) % buckets.size();
            buckets[_current_window].reset(now_ts);
        } else if (delta >= std::chrono::seconds(0)) {
            /// Keep last window open and adjust delta so next window closes
            /// that much sooner
            last_window_delta = delta;
        }
    }
    _buckets = std::move(buckets);
    return last_window_delta;
}

usage_manager::usage_manager(
  ss::sharded<cluster::health_monitor_frontend>& health_monitor,
  ss::sharded<storage::api>& storage)
  : _usage_enabled(config::shard_local_cfg().enable_usage.bind())
  , _usage_num_windows(config::shard_local_cfg().usage_num_windows.bind())
  , _usage_window_width_interval(
      config::shard_local_cfg().usage_window_width_interval_sec.bind())
  , _usage_disk_persistance_interval(
      config::shard_local_cfg().usage_disk_persistance_interval_sec.bind())
  , _health_monitor(health_monitor)
  , _storage(storage) {}

ss::future<> usage_manager::reset() {
    oncore_debug_verify(_verify_shard);
    try {
        auto h = _background_gate.hold();
        auto u = co_await _background_mutex.get_units();
        if (_accounting_fiber) {
            /// Deallocate the accounting_fiber if the feature is disabled,
            /// otherwise it will keep in memory the number of configured
            /// historical buckets
            auto accounting_fiber = std::exchange(_accounting_fiber, nullptr);
            co_await accounting_fiber->stop();
        }
        co_await start_accounting_fiber();
    } catch (ss::gate_closed_exception&) {
        // shutting down
    }
}

ss::future<> usage_manager::start_accounting_fiber() {
    if (!_usage_enabled()) {
        co_return; /// Feature is disabled in config
    }
    if (_accounting_fiber) {
        co_return; /// Double start called, do-nothing
    }
    _accounting_fiber = std::make_unique<accounting_fiber>(
      this->container(),
      _health_monitor,
      _storage,
      _usage_num_windows(),
      _usage_window_width_interval(),
      _usage_disk_persistance_interval());

    co_await _accounting_fiber->start();

    /// Reset all state
    co_await container().invoke_on_all(
      [](usage_manager& um) { (void)um.sample(); });
}

ss::future<> usage_manager::start() {
    if (ss::this_shard_id() != usage_manager_main_shard) {
        co_return; /// Async work only occurs on shard-0
    }
    co_await start_accounting_fiber();

    _usage_enabled.watch([this] { (void)reset(); });
    _usage_num_windows.watch([this] { (void)reset(); });
    _usage_window_width_interval.watch([this] { (void)reset(); });
    _usage_disk_persistance_interval.watch([this] { (void)reset(); });
}

ss::future<> usage_manager::stop() {
    co_await _background_gate.close();
    if (!_accounting_fiber) {
        co_return;
    }
    /// Logic could only possibly execute on core-0 since no other cores would
    /// initialize a local _accounting_fiber
    co_await _accounting_fiber->stop();
}

std::vector<usage_window> usage_manager::get_usage_stats() const {
    if (ss::this_shard_id() != usage_manager_main_shard) {
        throw std::runtime_error(
          "Attempt to query results of "
          "kafka::usage_manager off the main accounting shard");
    }
    if (!_accounting_fiber) {
        return {}; // no stats
    }
    return _accounting_fiber->get_usage_stats();
}

} // namespace kafka
