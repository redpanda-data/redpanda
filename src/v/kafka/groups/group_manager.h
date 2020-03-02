#pragma once
#include "config/configuration.h"
#include "kafka/errors.h"
#include "kafka/groups/group.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/sync_group_request.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>
#include <cluster/partition_manager.h>

namespace kafka {

/// \brief Manages the Kafka group lifecycle.
class group_manager {
public:
    group_manager(
      ss::sharded<cluster::partition_manager>& partitions,
      config::configuration& conf)
      : _partitions(partitions.local())
      , _conf(conf) {}

    ss::future<> start() {
        // TODO setup partition manager hooks when this shard becomes a
        // partition leader for the group membership topic then group state will
        // need to be recovered. this will happen for instance at startup, and
        // in response to failovers.
        return ss::make_ready_future<>();
    }

    ss::future<> stop() {
        // TODO clean up groups and members and timers. in flight requests may
        // be holding references to groups/members--so here we mark groups as
        // being cleaned up. we can also use semaphore / guards here, but this
        // integration is evolving.
        return ss::make_ready_future<>();
    }

public:
    /// \brief Handle a JoinGroup request
    ss::future<join_group_response> join_group(join_group_request&& request);

    /// \brief Handle a SyncGroup request
    ss::future<sync_group_response> sync_group(sync_group_request&& request);

    /// \brief Handle a Heartbeat request
    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    /// \brief Handle a LeaveGroup request
    ss::future<leave_group_response> leave_group(leave_group_request&& request);

public:
    static error_code validate_group_status(group_id group, api_key api);
    static bool valid_group_id(group_id group, api_key api);

private:
    group_ptr get_group(const group_id& group) {
        if (auto it = _groups.find(group); it != _groups.end()) {
            return it->second;
        }
        return nullptr;
    }

    cluster::partition_manager& _partitions;
    config::configuration& _conf;
    absl::flat_hash_map<group_id, group_ptr> _groups;
};

} // namespace kafka
