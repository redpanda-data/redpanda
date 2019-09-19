#pragma once

#include "hashing/jump_consistent_hash.h" // remove later
#include "raft/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>

namespace raft {
/// \brief contains a lookup table from group_id -> shard
struct group_shard_table {
    shard_id shard_for(group_id group) {
        return jump_consistent_hash(group(), smp::count);
    }
};
} // namespace raft
