/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "cluster/shard_table.h"
#include "features/feature_table.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/delete_groups_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <exception>
#include <type_traits>

namespace kafka {

/**
 * \brief Forward group operations the owning core.
 *
 * Routing an operation is a two step process. First, the coordinator key is
 * mapped to its associated ntp using the coordinator_ntp_mapper. Given the ntp
 * the owning shard is found using the cluster::shard_table. Finally, a x-core
 * operation on the destination shard's group manager is invoked.
 */
class group_router final {
public:
    group_router(
      ss::scheduling_group sched_group,
      ss::smp_service_group smp_group,
      ss::sharded<group_manager>& gr_manager,
      ss::sharded<cluster::shard_table>& shards,
      ss::sharded<coordinator_ntp_mapper>& coordinators)
      : _sg(sched_group)
      , _ssg(smp_group)
      , _group_manager(gr_manager)
      , _shards(shards)
      , _coordinators(coordinators) {}

    group::join_group_stages join_group(join_group_request&& request);

    group::sync_group_stages sync_group(sync_group_request&& request);

    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    ss::future<leave_group_response> leave_group(leave_group_request&& request);

    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request&& request);

    ss::future<offset_delete_response>
    offset_delete(offset_delete_request&& request);

    group::offset_commit_stages offset_commit(offset_commit_request&& request);

    ss::future<txn_offset_commit_response>
    txn_offset_commit(txn_offset_commit_request&& request);

    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request request);

    ss::future<cluster::begin_group_tx_reply>
    begin_tx(cluster::begin_group_tx_request request);

    ss::future<cluster::prepare_group_tx_reply>
    prepare_tx(cluster::prepare_group_tx_request request);

    ss::future<cluster::abort_group_tx_reply>
    abort_tx(cluster::abort_group_tx_request request);

    // return groups from across all shards, and if any core was still loading
    ss::future<std::pair<error_code, std::vector<listed_group>>> list_groups();

    ss::future<described_group> describe_group(kafka::group_id g);

    ss::future<std::vector<deletable_group_result>>
    delete_groups(std::vector<group_id> groups);

    ss::sharded<coordinator_ntp_mapper>& coordinator_mapper() {
        return _coordinators;
    }

    ss::sharded<group_manager>& get_group_manager() { return _group_manager; }

private:
    template<typename Request, typename FwdFunc>
    auto route(Request&& r, FwdFunc func);

    template<typename Request, typename FwdFunc>
    auto route_tx(Request&& r, FwdFunc func);

    template<typename Request, typename FwdFunc>
    auto route_stages(Request r, FwdFunc func);

    using sharded_groups = absl::
      node_hash_map<ss::shard_id, std::vector<std::pair<model::ntp, group_id>>>;

    std::optional<std::pair<model::ntp, ss::shard_id>>
    shard_for(const group_id& group) {
        if (auto ntp = coordinator_mapper().local().ntp_for(group); ntp) {
            if (auto shard_id = _shards.local().shard_for(*ntp); shard_id) {
                return std::make_pair(std::move(*ntp), *shard_id);
            }
        }
        return std::nullopt;
    }

    ss::future<std::vector<deletable_group_result>> route_delete_groups(
      ss::shard_id, std::vector<std::pair<model::ntp, group_id>>);

    ss::future<> parallel_route_delete_groups(
      std::vector<deletable_group_result>&, sharded_groups&);

    ss::scheduling_group _sg;
    ss::smp_service_group _ssg;
    ss::sharded<group_manager>& _group_manager;
    ss::sharded<cluster::shard_table>& _shards;
    ss::sharded<coordinator_ntp_mapper>& _coordinators;
};

} // namespace kafka
