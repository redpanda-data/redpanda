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

#include "cluster/data_migration_group_proxy.h"
#include "kafka/protocol/types.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_initializer.h"
#include "kafka/server/group_manager.h"

namespace kafka {

/*
 * Proxy component to operate consumer groups from cluster data migration
 * components.
 */
class data_migration_group_proxy_impl final
  : public cluster::data_migrations::group_proxy {
public:
    data_migration_group_proxy_impl(
      coordinator_ntp_mapper& coordinator_ntp_mapper,
      group_manager& group_manager,
      group_initializer& group_initializer);

    std::optional<model::partition_id>
    partition_for(const kafka::group_id& group) override;

    ss::future<result<model::offset>> set_blocked_for_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>& groups,
      bool to_block) override;

    ss::future<std::error_code> delete_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>& groups) override;

    virtual ss::future<bool>
    assure_topic_exists(model::timeout_clock::time_point deadline) override;

private:
    coordinator_ntp_mapper& _coordinator_ntp_mapper;
    group_manager& _group_manager;
    group_initializer& _group_initializer;
};
} // namespace kafka
