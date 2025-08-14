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

#include "kafka/server/data_migration_group_proxy_impl.h"

#include "model/timeout_clock.h"

namespace kafka {

/*
 * Proxy component to operate consumer groups from cluster data migration
 * components.
 */
data_migration_group_proxy_impl::data_migration_group_proxy_impl(
  coordinator_ntp_mapper& coordinator_ntp_mapper,
  group_manager& group_manager,
  group_initializer& group_initializer)
  : _coordinator_ntp_mapper(coordinator_ntp_mapper)
  , _group_manager(group_manager)
  , _group_initializer(group_initializer) {}

std::optional<model::partition_id>
data_migration_group_proxy_impl::partition_for(const kafka::group_id& group) {
    return _coordinator_ntp_mapper.partition_for(group);
}

ss::future<result<model::offset>>
data_migration_group_proxy_impl::set_blocked_for_groups(
  const model::ntp& co_ntp,
  const chunked_vector<kafka::group_id>& groups,
  bool to_block) {
    return _group_manager.set_blocked_for_groups(co_ntp, groups, to_block);
};

ss::future<std::error_code> data_migration_group_proxy_impl::delete_groups(
  const model::ntp& co_ntp, const chunked_vector<kafka::group_id>& groups) {
    return _group_manager.empty_and_delete_groups(co_ntp, groups);
}

ss::future<bool> data_migration_group_proxy_impl::assure_topic_exists(
  model::timeout_clock::time_point deadline) {
    return _group_initializer.assure_topic_exists(true, deadline);
}

} // namespace kafka
