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

#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "model/timeout_clock.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

class data_policy_frontend final {
public:
    data_policy_frontend(
      ss::sharded<controller_stm>&, ss::sharded<ss::abort_source>&) noexcept;

    ss::future<std::error_code> create_data_policy(
      model::topic_namespace,
      v8_engine::data_policy,
      model::timeout_clock::time_point);

    ss::future<std::error_code> clear_data_policy(
      model::topic_namespace, model::timeout_clock::time_point);

private:
    ss::sharded<controller_stm>& _stm;
    ss::sharded<ss::abort_source>& _as;
};

} // namespace cluster
