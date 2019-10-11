#pragma once

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>

namespace storage {

inline shard_id shard_of(const model::ntp& ntp) {
    incremental_xxhash64 inc;
    inc.update(ntp.ns());
    inc.update(ntp.tp.topic());
    auto p = ntp.tp.partition();
    inc.update((const char*)&p, sizeof(model::partition_id::type));
    return jump_consistent_hash(inc.digest(), smp::count);
}

} // namespace storage
