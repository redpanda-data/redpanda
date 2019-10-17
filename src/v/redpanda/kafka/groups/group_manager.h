#pragma once
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/join_group_request.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cluster/partition_manager.h>

namespace kafka::groups {

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
    future<requests::join_group_response> join_group(
      const requests::request_context& ctx,
      requests::join_group_request&& request) {
        using reply = requests::join_group_response;
        return make_ready_future<reply>(
          reply(request.member_id, kerr::unsupported_version));
    }

private:
    // kafka protocol error codes
    using kerr = errors::error_code;

    cluster::partition_manager& _partitions;
};

} // namespace kafka::groups
