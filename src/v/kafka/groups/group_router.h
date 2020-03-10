#pragma once
#include "cluster/shard_table.h"
#include "kafka/groups/coordinator_ntp_mapper.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/offset_commit_request.h"
#include "kafka/requests/offset_fetch_request.h"
#include "kafka/requests/sync_group_request.h"
#include "kafka/types.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

namespace kafka {

// clang-format off
CONCEPT(
template <typename T>
concept GroupManager =
requires(
  T m,
  join_group_request&& join_request,
  sync_group_request&& sync_request,
  heartbeat_request&& heartbeat_request,
  leave_group_request&& leave_request,
  offset_commit_request&& offset_commit_request,
  offset_fetch_request&& offset_fetch_request) {

    { m.join_group(std::move(join_request)) } ->
        ss::future<join_group_response>;

    { m.sync_group(std::move(sync_request)) } ->
        ss::future<sync_group_response>;

    { m.heartbeat(std::move(heartbeat_request)) } ->
        ss::future<heartbeat_response>;

    { m.leave_group(std::move(leave_request)) } ->
        ss::future<leave_group_response>;

    { m.offset_commit(std::move(offset_commit_request)) } ->
        ss::future<offset_commit_response>;

    { m.offset_fetch(std::move(offset_fetch_request)) } ->
        ss::future<offset_fetch_response>;
};
)
// clang-format on

/**
 * \brief Forward group operations the owning core.
 *
 * Routing an operation is a two step process. First, the coordinator key is
 * mapped to its associated ntp using the coordinator_ntp_mapper. Given the ntp
 * the owning shard is found using the cluster::shard_table. Finally, a x-core
 * operation on the destination shard's group manager is invoked.
 */
template<typename GroupMgr>
CONCEPT(requires GroupManager<GroupMgr>)
class group_router final {
public:
    group_router(
      ss::scheduling_group sched_group,
      ss::smp_service_group smp_group,
      ss::sharded<GroupMgr>& group_manager,
      ss::sharded<cluster::shard_table>& shards,
      ss::sharded<coordinator_ntp_mapper>& coordinators)
      : _sg(sched_group)
      , _ssg(smp_group)
      , _group_manager(group_manager)
      , _shards(shards)
      , _coordinators(coordinators) {}

    ss::future<join_group_response> join_group(join_group_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<join_group_response>(
              join_group_response(
                request.member_id, error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.join_group(std::move(request));
                });
          });
    }

    ss::future<sync_group_response> sync_group(sync_group_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<sync_group_response>(
              sync_group_response(error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.sync_group(std::move(request));
                });
          });
    }

    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<heartbeat_response>(
              heartbeat_response(error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.heartbeat(std::move(request));
                });
          });
    }

    ss::future<leave_group_response>
    leave_group(leave_group_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<leave_group_response>(
              leave_group_response(error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.leave_group(std::move(request));
                });
          });
    }

    ss::future<offset_commit_response>
    offset_commit(offset_commit_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<offset_commit_response>(
              offset_commit_response(request, error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.offset_commit(std::move(request));
                });
          });
    }

    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request&& request) {
        auto shard = shard_for(request.group_id);
        if (!shard) {
            return ss::make_ready_future<offset_fetch_response>(
              offset_fetch_response(error_code::not_coordinator));
        }
        return with_scheduling_group(
          _sg, [this, shard = *shard, request = std::move(request)]() mutable {
              return _group_manager.invoke_on(
                shard,
                _ssg,
                [request = std::move(request)](GroupMgr& m) mutable {
                    return m.offset_fetch(std::move(request));
                });
          });
    }

private:
    std::optional<ss::shard_id> shard_for(group_id group) {
        if (auto ntp = _coordinators.local().ntp_for(group); ntp) {
            return _shards.local().shard_for(*ntp);
        }
        return std::nullopt;
    }

    ss::scheduling_group _sg;
    ss::smp_service_group _ssg;
    ss::sharded<GroupMgr>& _group_manager;
    ss::sharded<cluster::shard_table>& _shards;
    ss::sharded<coordinator_ntp_mapper>& _coordinators;
};

} // namespace kafka
