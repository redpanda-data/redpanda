/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/scheduling/allocation_node.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, const allocation_node& n) {
    o << "{node:" << n._id << ", max_partitions_per_core: "
      << allocation_node::max_allocations_per_core
      << ", partition_capacity:" << n.partition_capacity() << ", weights: [";
    for (auto w : n._weights) {
        o << "(" << w << ")";
    }
    return o << "]}";
}

} // namespace cluster
