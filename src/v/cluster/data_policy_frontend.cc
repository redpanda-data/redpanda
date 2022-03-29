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

#include "cluster/data_policy_frontend.h"

#include "cluster/cluster_utils.h"

#include <optional>

namespace cluster {

data_policy_frontend::data_policy_frontend(
  ss::sharded<controller_stm>& stm, ss::sharded<ss::abort_source>& as) noexcept
  : _stm(stm)
  , _as(as) {}

ss::future<std::error_code> data_policy_frontend::create_data_policy(
  model::topic_namespace topic,
  v8_engine::data_policy dp,
  model::timeout_clock::time_point tout) {
    create_data_policy_cmd_data cmd_data{.dp = std::move(dp)};
    create_data_policy_cmd cmd(std::move(topic), std::move(cmd_data));
    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> data_policy_frontend::clear_data_policy(
  model::topic_namespace topic, model::timeout_clock::time_point tout) {
    delete_data_policy_cmd cmd(std::move(topic), std::nullopt);
    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

} // namespace cluster
