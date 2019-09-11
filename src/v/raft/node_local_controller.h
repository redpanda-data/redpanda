#pragma once

#include "hashing/jump_consistent_hash.h" // remove later
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>

namespace raft {
class node_local_controller {
public:
    shard_id shard_for_raft_group(group_id group) {
        // FIXME(agallego) - needs controller code
        return jump_consistent_hash(group(), smp::count);
    }
};
} // namespace raft
