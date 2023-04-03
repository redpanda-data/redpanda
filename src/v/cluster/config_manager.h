/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "model/record.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace YAML {
class Node;
}

namespace cluster {

/// This state machine receives updates to the global cluster config,
/// and uses them to update the per-shard configuration objects
/// that are consumed by other services within redpanda.
class config_manager final {
    static constexpr auto accepted_commands = make_commands_list<
      cluster_config_delta_cmd,
      cluster_config_status_cmd>{};

public:
    static constexpr ss::shard_id shard = 0;

    struct preload_result {
        config_version version{config_version_unset};
        std::map<ss::sstring, ss::sstring> raw_values;
        std::vector<ss::sstring> unknown;
        std::vector<ss::sstring> invalid;
    };

    using status_map = std::map<model::node_id, config_status>;

    config_manager(
      preload_result preload,
      ss::sharded<config_frontend>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::members_table>&,
      ss::sharded<ss::abort_source>&);

    static ss::future<preload_result> preload(YAML::Node const&);
    ss::future<> start();
    ss::future<> stop();

    // mux_state_machine interface
    bool is_batch_applicable(const model::record_batch& b);
    ss::future<std::error_code> apply_update(model::record_batch);
    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    // Result of trying to apply a delta to a configuration
    struct apply_result {
        bool restart{false};
        std::vector<ss::sstring> unknown;
        std::vector<ss::sstring> invalid;
    };

    status_map const& get_status() const { return status; }

    status_map get_projected_status() const;

    config_version get_version() const noexcept { return _seen_version; }

    bool needs_update(const config_status& new_status) {
        if (auto s = status.find(new_status.node); s != status.end()) {
            return s->second != new_status;
        } else {
            return true;
        }
    }

private:
    void merge_apply_result(
      config_status&,
      cluster_config_delta_cmd_data const&,
      apply_result const&);

    bool should_send_status();
    ss::future<> reconcile_status();
    ss::future<std::error_code> apply_delta(cluster_config_delta_cmd&&);
    ss::future<> store_delta(
      config_version const& version, cluster_config_delta_cmd_data const& data);

    bool _bootstrap_complete{false};
    void start_bootstrap();
    ss::future<> do_bootstrap();

    static ss::future<preload_result> load_cache();
    static ss::future<bool> load_bootstrap();
    static ss::future<> load_legacy(YAML::Node const&);

    ss::future<std::error_code> apply_status(cluster_config_status_cmd&& cmd);

    static std::filesystem::path bootstrap_path();
    static std::filesystem::path cache_path();
    ss::future<> wait_for_bootstrap();
    void handle_cluster_members_update(const std::vector<model::node_id>&);

    config_status my_latest_status;
    status_map status;

    model::node_id _self;
    config_version _seen_version{config_version_unset};
    std::map<ss::sstring, ss::sstring> _raw_values;

    ss::sharded<config_frontend>& _frontend;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<cluster::members_table>& _members;
    notification_id_type _member_removed_notification;
    notification_id_type _raft0_leader_changed_notification;

    ss::condition_variable _reconcile_wait;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
};

} // namespace cluster
