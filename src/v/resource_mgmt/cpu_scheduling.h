#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>

// manage cpu scheduling groups. scheduling groups are global, so one instance
// of this class can be created at the top level and passed down into any server
// and any shard that needs to schedule continuations into a given group.
class scheduling_groups final {
public:
    ss::future<> create_groups() {
        return ss::create_scheduling_group("admin", 100)
          .then([this](ss::scheduling_group sg) { _admin = sg; })
          .then([] { return ss::create_scheduling_group("raft", 1000); })
          .then([this](ss::scheduling_group sg) { _raft = sg; })
          .then([] { return ss::create_scheduling_group("kafka", 1000); })
          .then([this](ss::scheduling_group sg) { _kafka = sg; })
          .then([] { return ss::create_scheduling_group("cluster", 300); })
          .then([this](ss::scheduling_group sg) { _cluster = sg; });
    }

    ss::future<> destroy_groups() {
        return destroy_scheduling_group(_admin)
          .then([this] { return destroy_scheduling_group(_raft); })
          .then([this] { return destroy_scheduling_group(_kafka); })
          .then([this] { return destroy_scheduling_group(_cluster); });
    }

    ss::scheduling_group admin_sg() { return _admin; }
    ss::scheduling_group raft_sg() { return _raft; }
    ss::scheduling_group kafka_sg() { return _kafka; }
    ss::scheduling_group cluster_sg() { return _cluster; }

private:
    ss::scheduling_group _admin;
    ss::scheduling_group _raft;
    ss::scheduling_group _kafka;
    ss::scheduling_group _cluster;
};
