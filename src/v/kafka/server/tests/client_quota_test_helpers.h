// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"

#include <seastar/core/smp.hh>

#include <cstdint>

namespace kafka {

// Cluster configs used to be per-shard and for backwards compatibility,
// when we made them node-wide, we started acting on the cluster configs at
// the "core count" * "per-shard cluster config" value to be backwards
// compatible with the existing configurations.
// Therefore, in the unit tests, we need to scale various values by the core
// count to keep them passing.
inline uint64_t scale_to_smp_count(uint64_t val) {
    return val * ss::smp::count;
}

} // namespace kafka
