/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/shard_table.h"
#include "kafka/groups/coordinator_ntp_mapper.h"
#include "kafka/groups/group_manager.h"
#include "kafka/requests/describe_groups_request.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/list_groups_request.h"
#include "kafka/requests/offset_commit_request.h"
#include "kafka/requests/offset_fetch_request.h"
#include "kafka/requests/sync_group_request.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

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
    template<typename Request, typename FwdFunc>
    auto route(Request&& r, FwdFunc func) {
        // get response type from FwdFunc it has return future<response>.
        using return_type = std::invoke_result_t<
          FwdFunc,
          decltype(std::declval<group_manager>()),
          Request&&>;
        using resp_type = typename return_type::value_type;

        auto m = shard_for(r.data.group_id);
        if (!m) {
            return ss::make_ready_future<resp_type>(
              resp_type(r, error_code::not_coordinator));
        }
        r.ntp = std::move(m->first);
        return with_scheduling_group(
          _sg, [this, func, shard = m->second, r = std::move(r)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [func, r = std::move(r)](group_manager& mgr) mutable {
                    return std::invoke(func, mgr, std::move(r));
                });
          });
    }

public:
    group_router(
      ss::scheduling_group sched_group,
      ss::smp_service_group smp_group,
      ss::sharded<group_manager>& group_manager,
      ss::sharded<cluster::shard_table>& shards,
      ss::sharded<coordinator_ntp_mapper>& coordinators)
      : _sg(sched_group)
      , _ssg(smp_group)
      , _group_manager(group_manager)
      , _shards(shards)
      , _coordinators(coordinators) {}

    auto join_group(join_group_request&& request) {
        return route(std::move(request), &group_manager::join_group);
    }

    auto sync_group(sync_group_request&& request) {
        return route(std::move(request), &group_manager::sync_group);
    }

    auto heartbeat(heartbeat_request&& request) {
        return route(std::move(request), &group_manager::heartbeat);
    }

    auto leave_group(leave_group_request&& request) {
        return route(std::move(request), &group_manager::leave_group);
    }

    auto offset_commit(offset_commit_request&& request) {
        return route(std::move(request), &group_manager::offset_commit);
    }

    auto offset_fetch(offset_fetch_request&& request) {
        return route(std::move(request), &group_manager::offset_fetch);
    }

    // return groups from across all shards, and if any core was still loading
    ss::future<std::pair<bool, std::vector<listed_group>>> list_groups() {
        using type = std::pair<bool, std::vector<listed_group>>;
        return _group_manager.map_reduce0(
          [](group_manager& mgr) { return mgr.list_groups(); },
          type{},
          [](type a, type b) {
              // reduce into `a` and retain any affirmitive loading state
              a.first = a.first || b.first;
              a.second.insert(a.second.end(), b.second.begin(), b.second.end());
              return a;
          });
    }

    ss::future<described_group> describe_group(kafka::group_id g) {
        auto m = shard_for(g);
        if (!m) {
            return ss::make_ready_future<described_group>(
              describe_groups_response::make_empty_described_group(
                std::move(g), error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, g = std::move(g), m = std::move(m)]() mutable {
              return _group_manager.invoke_on(
                m->second,
                _ssg,
                [g = std::move(g),
                 ntp = std::move(m->first)](group_manager& mgr) mutable {
                    return mgr.describe_group(ntp, g);
                });
          });
    }

private:
    std::optional<std::pair<model::ntp, ss::shard_id>>
    shard_for(const group_id& group) {
        if (auto ntp = _coordinators.local().ntp_for(group); ntp) {
            if (auto shard_id = _shards.local().shard_for(*ntp); shard_id) {
                return std::make_pair(std::move(*ntp), *shard_id);
            }
        }
        return std::nullopt;
    }

    ss::scheduling_group _sg;
    ss::smp_service_group _ssg;
    ss::sharded<group_manager>& _group_manager;
    ss::sharded<cluster::shard_table>& _shards;
    ss::sharded<coordinator_ntp_mapper>& _coordinators;
};

} // namespace kafka
