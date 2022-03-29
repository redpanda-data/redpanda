/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>

namespace storage {

inline ss::shard_id shard_of(const model::ntp& ntp) {
    incremental_xxhash64 inc;
    inc.update(ntp.ns());
    inc.update(ntp.tp.topic());
    auto p = ntp.tp.partition();
    inc.update((const char*)&p, sizeof(model::partition_id::type));
    return jump_consistent_hash(inc.digest(), ss::smp::count);
}

} // namespace storage
