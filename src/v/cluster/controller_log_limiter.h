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
#include "ssx/sformat.h"
#include "utils/token_bucket.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace cluster {

struct limiter_configuration {
    config::binding<bool> enable;
    config::binding<size_t> topic_rps;
    config::binding<size_t> topic_capacity;
    config::binding<size_t> acls_and_users_rps;
    config::binding<size_t> acls_and_users_capacity;
    config::binding<size_t> node_management_rps;
    config::binding<size_t> node_management_capacity;
    config::binding<size_t> move_rps;
    config::binding<size_t> move_capacity;
    config::binding<size_t> configuration_rps;
    config::binding<size_t> configuration_capacity;
    config::binding<size_t> internal_rps;
};

class group_limiter {
public:
    explicit group_limiter(
      ss::sstring group_name,
      config::binding<size_t> rate_binding,
      config::binding<size_t> capacity)
      : _group_name(std::move(group_name))
      , _rate_binding(std::move(rate_binding))
      , _capacity(std::move(capacity))
      , _throttler(rate_binding(), group_name, capacity()) {
        _rate_binding.watch([this]() { update_rate(); });
        _capacity.watch([this]() { update_capacity(); });
        setup_public_metrics();
    }

    ss::future<bool> try_throttle() {
        return ss::make_ready_future<bool>(_throttler.try_throttle(1));
    }

    ss::future<bool> throttle(ss::sharded<ss::abort_source>& as) {
        bool error = false;
        _throttler.throttle(1, as.local())
          .handle_exception_type(
            [&error](const ss::broken_semaphore&) { error = true; })
          .get();
        if (error) {
            return ss::make_ready_future<bool>(false);
        }
        return ss::make_ready_future<bool>(true);
    }

private:
    void update_rate() { _throttler.update_rate(_rate_binding()); }

    void update_capacity() { _throttler.update_capacity(_capacity()); }

    void account_dropped() { _dropped_requests_amount += 1; }

    void setup_public_metrics() {
        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        namespace sm = ss::metrics;
        _public_metrics.add_group(
          "cluster:controller_limit",
          {sm::make_gauge(
             ssx::sformat("{}_requests_dropped", _group_name),
             [this] {
                 auto dropped = _dropped_requests_amount;
                 _dropped_requests_amount = 0;
                 return dropped;
             },
             sm::description(ssx::sformat(
               "Amount of dropped requests since last metric in {} group",
               _group_name))),
           sm::make_gauge(
             ssx::sformat("{}_requests_available_rps", _group_name),
             [this] { return _throttler.available(); },
             sm::description(
               ssx::sformat("Available rps for {} group", _group_name))),
           sm::make_gauge(
             ssx::sformat("{}_requests_queue_size", _group_name),
             [this] { return _throttler.waiters(); },
             sm::description(ssx::sformat(
               "Size of queue with requests for {} group", _group_name)))});
    }

    ss::sstring _group_name;
    config::binding<size_t> _rate_binding;
    config::binding<size_t> _capacity;
    token_bucket<> _throttler;
    size_t _dropped_requests_amount{};
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

class controller_log_limiter {
public:
    explicit controller_log_limiter(limiter_configuration configuration)
      : _enabled(std::move(configuration.enable))
      , _topic_operations_limiter(
          "topic_operations",
          configuration.topic_rps,
          configuration.topic_capacity)
      , _acls_and_users_operations_limiter(
          "acls_amd_users_operations",
          configuration.acls_and_users_rps,
          configuration.acls_and_users_rps)
      , _node_management_operations_limiter(
          "node_management_operations",
          configuration.node_management_rps,
          configuration.node_management_capacity)
      , _move_operations_limiter(
          "move_operations",
          configuration.move_rps,
          configuration.move_capacity)
      , _configuration_operations_limiter(
          "configuration_operations",
          configuration.configuration_rps,
          configuration.configuration_capacity)
      , _internal_operations_limiter(
          "internal_operations",
          configuration.internal_rps,
          configuration.internal_rps) {}

    template<typename Cmd>
    ss::future<bool> throttle(Cmd& cmd, ss::sharded<ss::abort_source>& as) {
        if (!_enabled()) {
            return ss::make_ready_future<bool>(true);
        }
        switch (cmd.batch_type) {
        case model::record_batch_type::topic_management_cmd:
            switch (cmd.type) {
            case create_topic_cmd_type:
            case delete_topic_cmd_type:
            case update_topic_properties_cmd_type:
            case create_partition_cmd_type:
            case create_non_replicable_topic_cmd_type:
                return _acls_and_users_operations_limiter.try_throttle();
            case move_partition_replicas_cmd_type:
            case cancel_moving_partition_replicas_cmd_type:
                return _move_operations_limiter.try_throttle();
            case finish_moving_partition_replicas_cmd_type:
                return _internal_operations_limiter.throttle(as);
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::user_management_cmd:
            switch (cmd.type) {
            case create_user_cmd_type:
            case delete_user_cmd_type:
            case update_user_cmd_type:
                return _topic_operations_limiter.try_throttle();
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::acl_management_cmd:
            switch (cmd.type) {
            case create_acls_cmd_type:
            case delete_acls_cmd_type:
                return _topic_operations_limiter.try_throttle();
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::data_policy_management_cmd:
            switch (cmd.type) {
            case create_data_policy_cmd_type:
            case delete_data_policy_cmd_type:
                return _configuration_operations_limiter.try_throttle();
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::node_management_cmd:
            switch (cmd.type) {
            case decommission_node_cmd_type:
            case recommission_node_cmd_type:
            case maintenance_mode_cmd_type:
                return _node_management_operations_limiter.try_throttle();
            case finish_reallocations_cmd_type:
                return _internal_operations_limiter.throttle(as);
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::cluster_config_cmd:
            switch (cmd.type) {
            case cluster_config_delta_cmd_type:
            case cluster_config_status_cmd_type:
                return _configuration_operations_limiter.try_throttle();
            default:
                return ss::make_ready_future<bool>(true);
            }
        case model::record_batch_type::feature_update:
            switch (cmd.type) {
            case feature_update_cmd_type:
            case feature_update_license_update_cmd_type:
                return _configuration_operations_limiter.try_throttle();
            default:
                return ss::make_ready_future<bool>(true);
            }
        default:
            return ss::make_ready_future<bool>(true);
        }
    }

private:
    config::binding<bool> _enabled;
    group_limiter _topic_operations_limiter;
    group_limiter _acls_and_users_operations_limiter;
    group_limiter _node_management_operations_limiter;
    group_limiter _move_operations_limiter;
    group_limiter _configuration_operations_limiter;
    group_limiter _internal_operations_limiter;
};

} // namespace cluster