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
#include "base/vlog.h"
#include "container/fragmented_vector.h"
#include "kafka/server/logger.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "storage/kvstore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace kafka {

namespace detail {

template<typename clock_type, typename duration>
std::chrono::time_point<clock_type, duration> round_to_interval(
  std::chrono::seconds usage_window_width_interval,
  std::chrono::time_point<clock_type, duration> t) {
    /// Downstream systems are particularly sensitive to minor issues with
    /// timestamps not triggering on the configured interval (hours, minutes,
    /// seconds, etc), this method rounds t to the nearest interval and logs an
    /// error if this cannot be done within some threshold.
    using namespace std::chrono_literals;
    const auto interval = usage_window_width_interval;
    const auto err_threshold = interval < 2min ? 2s : 2min;
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
      "threshold of {}s meaning a clock has fired later or earlier then "
      "expected, this is unexpected behavior and should be investigated.",
      t.time_since_epoch().count(),
      std::chrono::duration_cast<std::chrono::seconds>(err_threshold));
    return t;
}

} // namespace detail

/// Main structure of statistics that are being accounted for. These are
/// periodically serialized to disk, hence why the struct inherits from the
/// serde::envelope
struct usage
  : serde::envelope<usage, serde::version<0>, serde::compat_version<0>> {
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    std::optional<uint64_t> bytes_cloud_storage;
    usage operator+(const usage&) const;
    usage& operator+=(const usage&);
    auto serde_fields() {
        return std::tie(bytes_sent, bytes_received, bytes_cloud_storage);
    }
    friend bool operator==(const usage&, const usage&) = default;
    friend std::ostream& operator<<(std::ostream& os, const usage& u) {
        fmt::print(
          os,
          "{{ bytes_sent: {} bytes_received: {} bytes_cloud_storage: {} }}",
          u.bytes_sent,
          u.bytes_received,
          u.bytes_cloud_storage ? std::to_string(*u.bytes_cloud_storage)
                                : "n/a");
        return os;
    }
};

struct usage_window
  : serde::envelope<usage_window, serde::version<0>, serde::compat_version<0>> {
    uint64_t begin{0};
    uint64_t end{0};
    usage u;

    /// Only valid to assert these conditions internal to usage manager. When it
    /// returns windows as results these conditions may not hold.
    bool is_uninitialized() const { return begin == 0 && end == 0; }
    bool is_open() const { return begin != 0 && end == 0; }

    void reset(uint64_t now);

    auto serde_fields() { return std::tie(begin, end, u); }
};

template<typename clock_type = ss::lowres_clock>
class usage_aggregator {
public:
    /// This type represents the type of the timestamps that are taken when
    /// closing a window and filling in the value of the usage_windows begin/end
    /// values. Whereas the clock_type value is the value of the clock used by
    /// the ss::timer<>
    using timestamp_t = std::conditional_t<
      std::is_same_v<clock_type, ss::lowres_clock>,
      ss::lowres_system_clock,
      clock_type>;

    usage_aggregator(
      storage::kvstore& kvstore,
      size_t usage_num_windows,
      std::chrono::seconds usage_window_width_interval,
      std::chrono::seconds usage_disk_persistance_interval);
    virtual ~usage_aggregator() = default;

    virtual ss::future<> start();
    virtual ss::future<> stop();

    ss::future<std::vector<usage_window>> get_usage_stats();

    std::chrono::seconds max_history() const {
        return _usage_window_width_interval * _usage_num_windows;
    }

protected:
    /// Portions of the implementation that rely on higher level constructs
    /// to obtain the data are not relevent to unit testing this class and
    /// can be broken out so this class can be made more testable
    virtual ss::future<usage> close_current_window() = 0;

    /// When writing unit tests it will be useful to know exactly when the timer
    /// has been armed so that moving forward the manual clock may actually
    /// invoke the timer to fire.
    virtual void window_closed() {}

private:
    void reset_state(fragmented_vector<usage_window> buckets);
    void close_window();
    void rearm_window_timer();
    bool is_bucket_stale(size_t idx, uint64_t close_ts) const;
    ss::future<> grab_data(size_t);

private:
    size_t _usage_num_windows;
    std::chrono::seconds _usage_window_width_interval;
    std::chrono::seconds _usage_disk_persistance_interval;

    /// Timers for controlling window closure data fetching and disk persistance
    ss::timer<clock_type> _close_window_timer;
    ss::timer<clock_type> _persist_disk_timer;

    mutex _m{"usage_aggregator"};
    ss::gate _bg_write_gate;
    ss::gate _gate;
    size_t _current_window{0};
    fragmented_vector<usage_window> _buckets;
    storage::kvstore& _kvstore;
};
} // namespace kafka
