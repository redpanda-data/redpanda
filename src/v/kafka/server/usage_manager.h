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
#include "cluster/fwd.h"
#include "config/property.h"
#include "kafka/server/usage_aggregator.h"
#include "oncore.h"
#include "storage/fwd.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

namespace kafka {

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

    class usage_accounting_fiber final : public usage_aggregator<> {
    public:
        usage_accounting_fiber(
          cluster::controller* controller,
          ss::sharded<usage_manager>& um,
          ss::sharded<cluster::health_monitor_frontend>& health_monitor,
          ss::sharded<storage::api>& storage,
          size_t usage_num_windows,
          std::chrono::seconds usage_window_width_interval,
          std::chrono::seconds usage_disk_persistance_interval);

    protected:
        virtual ss::future<usage> close_current_window() final;

    private:
        ss::future<std::optional<uint64_t>> get_cloud_usage_data();

    private:
        cluster::controller* _controller;
        cluster::health_monitor_frontend& _health_monitor;
        ss::sharded<usage_manager>& _um;
    };

    /// Class constructor
    ///
    /// Context is to be in a sharded service, will grab \ref usage_num_windows
    /// and \ref usage_window_sec configuration parameters from cluster config
    explicit usage_manager(
      cluster::controller* controller,
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
    ss::future<std::vector<usage_window>> get_usage_stats() const;

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

    cluster::controller* _controller;
    ss::sharded<cluster::health_monitor_frontend>& _health_monitor;
    ss::sharded<storage::api>& _storage;

    /// Per-core metric, shard-0 aggregates these values across shards
    usage _current_bucket;
    mutex _background_mutex;
    ss::gate _background_gate;

    /// Valid on core-0 when usage_enabled() == true
    std::unique_ptr<usage_aggregator<>> _accounting_fiber;
};

/// Bytes accounted for by the kafka::usage_manager will not take into
/// consideration data batches via produce/fetch requests to these topics
static const auto usage_excluded_topics = std::to_array(
  {model::topic("_schemas"),
   model::topic("__audit"),
   model::topic("__redpanda_e2e_probe")});

} // namespace kafka
