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
}

bool group_limiter::try_throttle() {
    vlog(
      controller_rate_limiter_log.debug,
      "Controller log request try throttle {}, token available: {}",
      _group_name,
      _throttler.available());
    return _throttler.try_throttle(1);
}

void group_limiter::update_rate() { _throttler.update_rate(_rate_binding()); }

void group_limiter::update_capacity() {
    if (_capacity_binding() == std::nullopt) {
        _throttler.update_capacity(_rate_binding());
    } else {
        _throttler.update_capacity(_capacity_binding().value());
    }
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
