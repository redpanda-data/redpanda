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
#include "bytes/oncore.h"
#include "config/property.h"
#include "storage/kvstore.h"
#include "utils/fragmented_vector.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace kafka {

/// Main structure of statistics that are being accounted for. These are
/// periodically serialized to disk, hence why the struct inherits from the
/// serde::envelope
struct usage
  : serde::envelope<usage, serde::version<0>, serde::compat_version<0>> {
    uint64_t bytes_sent{0};
    uint64_t bytes_received{0};
    std::optional<uint64_t> bytes_cloud_storage;
    usage operator+(const usage&) const;
    auto serde_fields() {
        return std::tie(bytes_sent, bytes_received, bytes_cloud_storage);
    }
};

struct usage_window
  : serde::envelope<usage_window, serde::version<0>, serde::compat_version<0>> {
    uint64_t begin{0};
    uint64_t end{0};
    usage u;

    bool is_uninitialized() const { return begin == 0 && end == 0; }
    bool is_open() const { return begin != 0 && end == 0; }
    void reset(ss::lowres_system_clock::time_point now);
    auto serde_fields() { return std::tie(begin, end, u); }
};

/// Class that manages all usage statistics. Usage stats are more accurate
/// representation of node usage. This class maintains these stats windowed,
/// a static number of windows that last some predefined interval, all of which
/// are options that can be configured at runtime.
///
/// Periodically these stats are written to disk using the kvstore at a 5min
/// interval, that way they are somewhat durable in the event of a node crash,
/// or restart.
class usage_manager : public ss::peering_sharded_service<usage_manager> {
public:
    static constexpr ss::shard_id usage_manager_main_shard{0};

    class accounting_fiber {
    public:
        accounting_fiber(
          ss::sharded<usage_manager>& um,
          ss::sharded<cluster::health_monitor_frontend>& health_monitor,
          ss::sharded<storage::api>& storage,
          size_t usage_num_windows,
          std::chrono::seconds usage_window_width_interval,
          std::chrono::seconds usage_disk_persistance_interval);

        /// Starts a fiber that manage window interval evolution and disk
        /// persistance
        ss::future<> start();

        /// Stops the fiber and flushes current in memory results to disk
        ss::future<> stop();

        /// Returns aggregate of all \ref usage stats across cores
        std::vector<usage_window> get_usage_stats() const;

    private:
        std::chrono::seconds
        reset_state(fragmented_vector<usage_window> buckets);
        void close_window();
        ss::future<> async_data_fetch(size_t index, uint64_t close_ts);

    private:
        size_t _usage_num_windows;
        std::chrono::seconds _usage_window_width_interval;
        std::chrono::seconds _usage_disk_persistance_interval;

        /// Timers for controlling window closure and disk persistance
        ss::timer<ss::lowres_clock> _timer;
        ss::timer<ss::lowres_clock> _persist_disk_timer;

        ss::gate _bg_write_gate;
        ss::gate _gate;
        size_t _current_window{0};
        fragmented_vector<usage_window> _buckets;
        cluster::health_monitor_frontend& _health_monitor;
        storage::kvstore& _kvstore;
        ss::sharded<usage_manager>& _um;
    };

    /// Class constructor
    ///
    /// Context is to be in a sharded service, will grab \ref usage_num_windows
    /// and \ref usage_window_sec configuration parameters from cluster config
    explicit usage_manager(
      ss::sharded<cluster::health_monitor_frontend>& health_monitor,
      ss::sharded<storage::api>& storage);

    /// Allocates and starts the accounting fiber
    ss::future<> start();

    /// Safely shuts down accounting fiber and deallocates it
    ss::future<> stop();

    /// Adds bytes to current open window
    ///
    /// Should be called at the kafka layer to account for bytes sent via kafka
    /// port
    void add_bytes_sent(size_t sent) { _current_bucket.bytes_sent += sent; }

    /// Adds bytes received to current open window
    ///
    /// Should be called at the kafka layer to account for bytes received via
    /// kafka port
    void add_bytes_recv(size_t recv) { _current_bucket.bytes_received += recv; }

    /// Obtain all current stats - for all shards
    ///
    std::vector<usage_window> get_usage_stats() const;

    /// Obtain all current stats - for 'this' shard, resets window
    ///
    usage sample() {
        usage u{};
        std::swap(_current_bucket, u);
        return u;
    }

private:
    /// Called when config options are modified
    ss::future<> reset();

    ss::future<> start_accounting_fiber();

    expression_in_debug_mode(oncore _verify_shard);

private:
    /// Config bindings, usage is visibility::user, others ::tunable
    config::binding<bool> _usage_enabled;
    config::binding<size_t> _usage_num_windows;
    config::binding<std::chrono::seconds> _usage_window_width_interval;
    config::binding<std::chrono::seconds> _usage_disk_persistance_interval;

    ss::sharded<cluster::health_monitor_frontend>& _health_monitor;
    ss::sharded<storage::api>& _storage;

    /// Per-core metric, shard-0 aggregates these values across shards
    usage _current_bucket;
    mutex _background_mutex;
    ss::gate _background_gate;

    /// Valid on core-0 when usage_enabled() == true
    std::unique_ptr<accounting_fiber> _accounting_fiber;
};

/// Bytes accounted for by the kafka::usage_manager will not take into
/// consideration data batches via produce/fetch requests to these topics
static const auto usage_excluded_topics = std::to_array(
  {model::topic("_schemas"),
   model::topic("__audit"),
   model::topic("__redpanda_e2e_probe")});

} // namespace kafka
