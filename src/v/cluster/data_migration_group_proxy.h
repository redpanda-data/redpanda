/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "cluster/errc.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

namespace cluster::data_migrations {

/*
 * Abstract class for a proxy component to operate kafka layer consumer groups
 * from data migration components.
 */
class group_proxy {
public:
    virtual ~group_proxy() = default;

    virtual std::optional<model::partition_id>
    partition_for(const kafka::group_id& group) = 0;

    virtual ss::future<result<model::offset>> set_blocked_for_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>&,
      bool to_block)
      = 0;

    virtual ss::future<std::error_code> delete_groups(
      const model::ntp& co_ntp, const chunked_vector<kafka::group_id>& groups)
      = 0;

    virtual ss::future<bool>
    assure_topic_exists(model::timeout_clock::time_point deadline) = 0;
};
} // namespace cluster::data_migrations
