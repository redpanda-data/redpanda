#pragma once
#include "seastarx.h"

#include <seastar/core/scheduling.hh>

// manage cpu scheduling groups. scheduling groups are global, so one instance
// of this class can be created at the top level and passed down into any server
// and any shard that needs to schedule continuations into a given group.
class scheduling_groups final {
public:
    future<> create_groups() {
        return create_scheduling_group("admin", 100)
          .then([this](scheduling_group sg) { _admin = sg; })
          .then([] { return create_scheduling_group("raft", 1000); })
          .then([this](scheduling_group sg) { _raft = sg; })
          .then([] { return create_scheduling_group("kafka", 1000) ; })
          .then([this](scheduling_group sg) { _kafka = sg; });
    }

    scheduling_group admin_sg() {
        return _admin;
    }
    scheduling_group raft_sg() {
        return _raft;
    }
    scheduling_group kafka_sg() {
        return _kafka;
    }

private:
    scheduling_group _admin;
    scheduling_group _raft;
    scheduling_group _kafka;
};
