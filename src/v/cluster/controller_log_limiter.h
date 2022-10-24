/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "config/configuration.h"
#include "ssx/metrics.h"
#include "utils/token_bucket.h"
#include "vlog.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace cluster {

inline ss::logger controller_rate_limiter_log("controller_rate_limiter_log");

struct limiter_configuration {
    config::binding<bool> enable;
    config::binding<size_t> topic_rps;
    config::binding<std::optional<size_t>> topic_capacity;
    config::binding<size_t> acls_and_users_rps;
    config::binding<std::optional<size_t>> acls_and_users_capacity;
    config::binding<size_t> node_management_rps;
    config::binding<std::optional<size_t>> node_management_capacity;
    config::binding<size_t> move_rps;
    config::binding<std::optional<size_t>> move_capacity;
    config::binding<size_t> configuration_rps;
    config::binding<std::optional<size_t>> configuration_capacity;
};

class group_limiter {
public:
    explicit group_limiter(
      ss::sstring group_name,
      config::binding<size_t> rate_binding,
      config::binding<std::optional<size_t>> capacity_binding);

    bool try_throttle();

private:
    void update_rate();
    void update_capacity();
    void setup_public_metrics();

    void account_dropped() { _dropped_requests_amount += 1; }

    ss::sstring _group_name;
    config::binding<size_t> _rate_binding;
    config::binding<std::optional<size_t>> _capacity_binding;
    token_bucket<> _throttler;
    int64_t _dropped_requests_amount{};
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

class controller_log_limiter {
public:
    explicit controller_log_limiter(limiter_configuration configuration);

    template<typename Cmd>
    bool throttle() {
        if (!_enabled()) {
            return true;
        }
        if constexpr (
          std::is_same_v<Cmd, create_topic_cmd> ||            //
          std::is_same_v<Cmd, delete_topic_cmd> ||            //
          std::is_same_v<Cmd, update_topic_properties_cmd> || //
          std::is_same_v<Cmd, create_partition_cmd> ||        //
          std::is_same_v<Cmd, create_non_replicable_topic_cmd>) {
            return _topic_operations_limiter.try_throttle();
        } else if constexpr (
          std::is_same_v<Cmd, move_partition_replicas_cmd> || //
          std::is_same_v<Cmd, cancel_moving_partition_replicas_cmd>) {
            return _move_operations_limiter.try_throttle();
        } else if constexpr (
          std::is_same_v<Cmd, create_user_cmd> || //
          std::is_same_v<Cmd, delete_user_cmd> || //
          std::is_same_v<Cmd, update_user_cmd> || //
          std::is_same_v<Cmd, create_acls_cmd> || //
          std::is_same_v<Cmd, delete_acls_cmd>) {
            return _acls_and_users_operations_limiter.try_throttle();
        } else if constexpr (
          std::is_same_v<Cmd, create_data_policy_cmd> ||   //
          std::is_same_v<Cmd, delete_data_policy_cmd> ||   //
          std::is_same_v<Cmd, cluster_config_delta_cmd> || //
          std::is_same_v<Cmd, feature_update_license_update_cmd>) {
            return _configuration_operations_limiter.try_throttle();
        } else if constexpr (
          std::is_same_v<Cmd, maintenance_mode_cmd> ||  //
          std::is_same_v<Cmd, recommission_node_cmd> || //
          std::is_same_v<Cmd, decommission_node_cmd>) {
            return _node_management_operations_limiter.try_throttle();
        } else {
            return true;
        }
    }

private:
    config::binding<bool> _enabled;
    group_limiter _topic_operations_limiter;
    group_limiter _acls_and_users_operations_limiter;
    group_limiter _node_management_operations_limiter;
    group_limiter _move_operations_limiter;
    group_limiter _configuration_operations_limiter;
};

} // namespace cluster
