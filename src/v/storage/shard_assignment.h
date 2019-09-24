#pragma once

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>

namespace storage {

inline shard_id shard_of(const model::namespaced_topic_partition& ntp) {
    incremental_xxhash64 inc;
    inc.update(ntp.ns.name);
    inc.update(ntp.tp.topic.name);
    auto p = ntp.tp.partition();
    inc.update((const char*)&p, sizeof(model::partition_id::type));
    return jump_consistent_hash(inc.digest(), smp::count);
}

} // namespace storage