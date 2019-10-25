#pragma once
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/heartbeat_request.h"
#include "redpanda/kafka/requests/join_group_request.h"
#include "redpanda/kafka/requests/leave_group_request.h"
#include "redpanda/kafka/requests/sync_group_request.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cluster/partition_manager.h>

namespace kafka {

static inline const kafka::member_id no_leader("");
static inline const kafka::generation_id no_generation(-1);
static inline const kafka::protocol_name no_protocol("");

/// \brief Manages the Kafka group lifecycle.
class group_manager {
public:
    group_manager(sharded<cluster::partition_manager>& partitions)
      : _partitions(partitions.local()) {
    }

    future<> start() {
        // TODO setup partition manager hooks when this shard becomes a
        // partition leader for the group membership topic then group state will
        // need to be recovered. this will happen for instance at startup, and
        // in response to failovers.
        return make_ready_future<>();
    }

    future<> stop() {
        // TODO clean up groups and members and timers. in flight requests may
        // be holding references to groups/members--so here we mark groups as
        // being cleaned up. we can also use semaphore / guards here, but this
        // integration is evolving.
        return make_ready_future<>();
    }

public:
    /// \brief Handle a JoinGroup request
    future<join_group_response> join_group(join_group_request&& request) {
        return join_error(request.member_id, error_code::unsupported_version);
    }

    /// \brief Handle a SyncGroup request
    future<sync_group_response> sync_group(sync_group_request&& request) {
        using reply = sync_group_response;
        return make_ready_future<reply>(reply(error_code::unsupported_version));
    }

    /// \brief Handle a Heartbeat request
    future<heartbeat_response> heartbeat(heartbeat_request&& request) {
        using reply = heartbeat_response;
        return make_ready_future<reply>(reply(error_code::unsupported_version));
    }

    /// \brief Handle a LeaveGroup request
    future<leave_group_response> leave_group(leave_group_request&& request) {
        using reply = leave_group_response;
        return make_ready_future<reply>(reply(error_code::unsupported_version));
    }

private:
    static future<join_group_response>
    join_error(kafka::member_id member_id, error_code error) {
        join_group_response response(
          error, no_generation, no_protocol, no_leader, std::move(member_id));
        return make_ready_future<join_group_response>(std::move(response));
    }

private:
    cluster::partition_manager& _partitions;
};

} // namespace kafka
