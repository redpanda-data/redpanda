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

#include "cluster/controller_log_limiter.h"

#include "cluster/commands.h"
#include "ssx/sformat.h"

#include <optional>

namespace cluster {

group_limiter::group_limiter(
  ss::sstring group_name,
  config::binding<size_t> rate_binding,
  config::binding<std::optional<size_t>> capacity_binding)
  : _group_name(std::move(group_name))
  , _rate_binding(std::move(rate_binding))
  , _capacity_binding(std::move(capacity_binding))
  , _throttler(_rate_binding(), _group_name, _rate_binding()) {
    // By default capacity should be equal to rate
    update_capacity();
    _rate_binding.watch([this]() { update_rate(); });
    _capacity_binding.watch([this]() { update_capacity(); });
    setup_public_metrics();
}

bool group_limiter::try_throttle() {
    vlog(
      controller_rate_limiter_log.trace,
      "Controller log request try throttle {}, token available: {}",
      _group_name,
      _throttler.available());
    bool throttle_success = _throttler.try_throttle(1);
    if (!throttle_success) {
        account_dropped();
    }
    return throttle_success;
}

void group_limiter::update_rate() { _throttler.update_rate(_rate_binding()); }

void group_limiter::update_capacity() {
    if (_capacity_binding() == std::nullopt) {
        _throttler.update_capacity(_rate_binding());
    } else {
        _throttler.update_capacity(_capacity_binding().value());
    }
}

void group_limiter::setup_public_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    auto group_label = ssx::metrics::make_namespaced_label("cmd_group");
    const std::vector<sm::label_instance> labels = {group_label(_group_name)};
    _public_metrics.add_group(
      "cluster_controller_log_limit",
      {sm::make_counter(
         "requests_dropped",
         [this] { return _dropped_requests_amount; },
         sm::description("Controller log rate limiting. "
                         "Amount of requests that are dropped "
                         "due to exceeding limit in group"),
         labels)
         .aggregate({sm::shard_label}),
       sm::make_gauge(
         "requests_available_rps",
         [this] { return _throttler.available(); },
         sm::description("Controller log rate limiting."
                         " Available rps for group"),
         labels)
         .aggregate({sm::shard_label})});
}

controller_log_limiter::controller_log_limiter(
  limiter_configuration configuration)
  : _enabled(std::move(configuration.enable))
  , _topic_operations_limiter(
      "topic_operations", configuration.topic_rps, configuration.topic_capacity)
  , _acls_and_users_operations_limiter(
      "acls_and_users_operations",
      configuration.acls_and_users_rps,
      configuration.acls_and_users_capacity)
  , _node_management_operations_limiter(
      "node_management_operations",
      configuration.node_management_rps,
      configuration.node_management_capacity)
  , _move_operations_limiter(
      "move_operations", configuration.move_rps, configuration.move_capacity)
  , _configuration_operations_limiter(
      "configuration_operations",
      configuration.configuration_rps,
      configuration.configuration_capacity) {}

} // namespace cluster
