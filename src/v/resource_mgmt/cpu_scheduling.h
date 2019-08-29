#pragma once
#include "redpanda/config/configuration.h"

#include <seastar/core/scheduling.hh>

// manage cpu scheduling groups. scheduling groups are global, so one instance
// of this class can be created at the top level and passed down into any server
// and any shard that needs to schedule continuations into a given group.
class scheduling_groups final {
public:
    future<> start(config::configuration& conf) {
        if (!conf.use_scheduling_groups()) {
            // results in default scheduling group being used
            return seastar::make_ready_future<>();
        }
        return seastar::create_scheduling_group("admin", 100)
          .then([this](scheduling_group sg) { _admin = sg; });
    }

    seastar::scheduling_group admin() {
        return _admin;
    }

private:
    seastar::scheduling_group _admin;
};
