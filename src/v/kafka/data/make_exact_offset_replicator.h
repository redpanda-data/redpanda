/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/shared_ptr.hh>

#include <memory>

namespace cluster {
class partition;
}

namespace kafka {

class exact_offset_replicator;

/// Create an exact_offset_replicator for the given partition.
/// Returns nullptr if the partition does not support offset-precise
/// replication (e.g. read replicas).
std::unique_ptr<exact_offset_replicator>
make_exact_offset_replicator(const ss::lw_shared_ptr<cluster::partition>&);

} // namespace kafka
